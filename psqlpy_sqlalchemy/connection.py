import asyncio
import re
import sys
import time
import typing as t
import uuid
from collections import deque
from functools import lru_cache
from typing import Any, Final

import psqlpy
from psqlpy import row_factories
from sqlalchemy import util
from sqlalchemy.connectors.asyncio import (
    AsyncAdapt_dbapi_connection,
    AsyncAdapt_dbapi_cursor,
    AsyncAdapt_dbapi_ss_cursor,
)
from sqlalchemy.dialects.postgresql.base import PGExecutionContext
from sqlalchemy.util.concurrency import await_only

# Python version for conditional optimizations
_PY_VERSION = sys.version_info[:2]

# Compiled regex patterns - use Final for JIT optimization (3.13+)
_PARAM_PATTERN: Final = re.compile(r":([a-zA-Z_][a-zA-Z0-9_]*)(::[\w\[\]]+)?")
_POSITIONAL_CHECK: Final = re.compile(r"\$\d+:$")
_UUID_PATTERN: Final = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
_VALUES_PATTERN: Final = re.compile(r"VALUES\s*\([^)]*\)", re.IGNORECASE)

# DML keywords as frozenset for O(1) lookup
_DML_KEYWORDS: Final[frozenset[str]] = frozenset(
    ("INSERT", "UPDATE", "DELETE")
)

# Pre-compute UUID class for faster comparison
_UUID_CLASS: Final = uuid.UUID


@lru_cache(maxsize=256)
def _get_param_regex(name: str) -> re.Pattern[str]:
    """Cached regex pattern for parameter substitution."""
    return re.compile(rf":({re.escape(name)})(::[\w\[\]]+)?")


# UUID conversion helper for psqlpy binary protocol compatibility
def _convert_uuid(val: t.Any) -> t.Any:
    """Convert UUID strings to UUID objects for psqlpy binary protocol.

    psqlpy uses the binary protocol which requires UUID values to be
    passed as uuid.UUID objects (not strings). This function ensures
    any UUID-formatted strings are converted to proper UUID objects.
    UUID objects are passed through unchanged.
    """
    if isinstance(val, _UUID_CLASS):
        # Already a UUID object, pass through
        return val
    if isinstance(val, str) and _UUID_PATTERN.match(val):
        try:
            return _UUID_CLASS(val)
        except ValueError:
            return val
    return val


# Optimized string operations for 3.12+
if _PY_VERSION >= (3, 12):

    def _check_dml(query: str) -> tuple[bool, str]:
        """Check if query is DML and return uppercase version."""
        q_upper = query.upper()
        start = q_upper.lstrip()[:6]
        return start in _DML_KEYWORDS and "RETURNING" not in q_upper, q_upper
else:

    def _check_dml(query: str) -> tuple[bool, str]:
        q_upper = query.upper()
        start = q_upper.lstrip()[:6]
        is_dml = start in _DML_KEYWORDS and "RETURNING" not in q_upper
        return is_dml, q_upper


if t.TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import (
        DBAPICursor,
        _DBAPICursorDescription,
    )


class PGExecutionContext_psqlpy(PGExecutionContext):
    def create_server_side_cursor(self) -> "DBAPICursor":
        return self._dbapi_connection.cursor(server_side=True)


class AsyncAdapt_psqlpy_cursor(AsyncAdapt_dbapi_cursor):
    __slots__ = (
        "_adapt_connection",
        "_arraysize",
        "_connection",
        "_cursor",
        "_description",
        "_invalidate_schema_cache_asof",
        "_rowcount",
        "_rows",
        "await_",
    )

    _adapt_connection: "AsyncAdapt_psqlpy_connection"
    _connection: psqlpy.Connection  # type: ignore[assignment]
    _cursor: t.Any | None  # type: ignore[assignment]
    _awaitable_cursor_close: bool = False

    def __init__(
        self, adapt_connection: "AsyncAdapt_psqlpy_connection"
    ) -> None:
        self._adapt_connection = adapt_connection
        self._connection = adapt_connection._connection
        self.await_ = adapt_connection.await_
        self._rows: deque[t.Any] = deque()
        self._cursor = None
        self._description: list[tuple[t.Any, ...]] | None = None
        self._arraysize = 1
        self._rowcount = -1
        self._invalidate_schema_cache_asof = 0

    async def _prepare_execute(
        self,
        querystring: str,
        parameters: t.Sequence[t.Any] | t.Mapping[str, Any] | None = None,
    ) -> None:
        """Execute a prepared statement."""
        if not self._adapt_connection._started:
            await self._adapt_connection._start_transaction()

        converted_query, converted_params = self._convert_params_single_pass(
            querystring, parameters
        )

        try:
            # DML without RETURNING: use execute() directly
            is_dml, _ = _check_dml(converted_query)
            if is_dml:
                await self._connection.execute(
                    converted_query, converted_params, prepared=True
                )
                self._description = None
                self._rowcount = 1
                self._rows = deque()
                return

            # SELECT/complex: use prepare() for column metadata
            prepared_stmt = await self._connection.prepare(
                querystring=converted_query,
                parameters=converted_params,
            )

            self._description = [
                (col.name, col.table_oid, None, None, None, None, None)
                for col in prepared_stmt.columns()
            ]

            if self.server_side:
                self._cursor = self._connection.cursor(
                    converted_query,
                    converted_params,
                )
                await self._cursor.start()
                self._rowcount = -1
                return

            results = await prepared_stmt.execute()

            # Use tuple unpacking directly - faster in Python 3.11+
            self._rows = deque(
                tuple(v for _, v in row)
                for row in results.row_factory(row_factories.tuple_row)
            )
            self._rowcount = len(self._rows)

        except Exception:
            self._description = None
            self._rowcount = -1
            self._rows = deque()
            self._adapt_connection._connection_valid = False
            raise

    def _process_parameters(
        self,
        parameters: t.Sequence[t.Any] | t.Mapping[str, Any] | None = None,
    ) -> t.Sequence[t.Any] | t.Mapping[str, Any] | None:
        """Process parameters for type conversion (legacy).

        Note: UUID conversion is now handled by dialect's bind processor,
        so this method is effectively a pass-through for most types.
        """
        if parameters is None:
            return None

        # No type conversion needed - dialect handles it
        return parameters

    def _convert_params_single_pass(
        self,
        querystring: str,
        parameters: t.Sequence[t.Any] | t.Mapping[str, Any] | None = None,
    ) -> tuple[str, list[Any] | None]:
        """Single-pass conversion: named→positional + UUID→bytes."""
        if parameters is None:
            return querystring, None

        # Fast path: already positional (list/tuple)
        if isinstance(parameters, list | tuple):
            return querystring, [_convert_uuid(v) for v in parameters]

        # Dict parameters: need named→positional conversion
        if not isinstance(parameters, dict):
            return querystring, None

        # Fast path: no named params in query
        if ":" not in querystring:
            return querystring, [_convert_uuid(v) for v in parameters.values()]

        # Find all parameter references
        matches = _PARAM_PATTERN.findall(querystring)
        if not matches:
            return querystring, [_convert_uuid(v) for v in parameters.values()]

        # Build param order (first occurrence wins)
        param_order: list[str] = []
        seen: set[str] = set()
        for name, _ in matches:
            if name not in seen and name in parameters:
                param_order.append(name)
                seen.add(name)

        # Check for missing params
        for name, _ in matches:
            if name not in parameters:
                return querystring, list(parameters.values())

        # Build converted params + query replacement
        converted_params = [
            _convert_uuid(parameters[name]) for name in param_order
        ]

        converted_query = querystring
        for i, name in enumerate(param_order, 1):
            converted_query = _get_param_regex(name).sub(
                f"${i}\\2", converted_query
            )

        return converted_query, converted_params

    def _convert_named_params_with_casting(
        self,
        querystring: str,
        parameters: t.Sequence[t.Any] | t.Mapping[str, Any] | None = None,
    ) -> tuple[str, t.Sequence[t.Any] | t.Mapping[str, Any] | None]:
        """Convert named parameters to positional (without UUID conversion)."""
        if parameters is None or not isinstance(parameters, dict):
            return querystring, parameters

        if ":" not in querystring:
            return querystring, parameters

        matches = _PARAM_PATTERN.findall(querystring)
        if not matches:
            return querystring, parameters

        param_order: list[str] = []
        seen: set[str] = set()
        for name, _ in matches:
            if name not in seen and name in parameters:
                param_order.append(name)
                seen.add(name)

        for name, _ in matches:
            if name not in parameters:
                return querystring, parameters

        converted_params = [parameters[name] for name in param_order]
        converted_query = querystring
        for i, name in enumerate(param_order, 1):
            converted_query = _get_param_regex(name).sub(
                f"${i}\\2", converted_query
            )

        return converted_query, converted_params

    @property
    def description(self) -> "_DBAPICursorDescription | None":
        return self._description

    @property
    def rowcount(self) -> int:
        return self._rowcount

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._arraysize = value

    async def _executemany(
        self,
        operation: str,
        seq_of_parameters: t.Sequence[t.Sequence[t.Any]],
    ) -> None:
        """Execute a batch of parameter sets."""
        if not self._adapt_connection._started:
            await self._adapt_connection._start_transaction()

        # Fast conversion using comprehension (inlined in 3.12+)
        converted_seq = [
            [
                _convert_uuid(v)
                for v in (p.values() if isinstance(p, dict) else p or [])
            ]
            for p in seq_of_parameters
        ]

        # INSERT: multi-value optimization
        is_dml, q_upper = _check_dml(operation)
        if len(converted_seq) > 1 and q_upper.lstrip().startswith("INSERT"):
            try:
                idx = 1
                parts = []
                flat: list[Any] = []
                for row in converted_seq:
                    n = len(row)
                    parts.append(
                        f"({', '.join(f'${i}' for i in range(idx, idx + n))})"
                    )
                    flat.extend(row)
                    idx += n

                query = _VALUES_PATTERN.sub(
                    f"VALUES {', '.join(parts)}", operation
                )
                await self._connection.execute(query, flat)
                self._rowcount = len(converted_seq)
                return
            except Exception:
                pass

        await self._connection.execute_many(
            operation, converted_seq, prepared=True
        )
        self._rowcount = len(converted_seq)

    def execute(
        self,
        operation: t.Any,
        parameters: t.Sequence[t.Any] | t.Mapping[str, Any] | None = None,
    ) -> None:
        # Auto-detect batch operations: if parameters is a list of dicts/tuples,
        # treat it as executemany for better performance
        if (
            isinstance(parameters, list)
            and len(parameters) > 1
            and all(isinstance(p, dict | tuple) for p in parameters)
        ):
            self.await_(self._executemany(operation, parameters))
        else:
            self.await_(self._prepare_execute(operation, parameters))

    def executemany(
        self, operation: t.Any, seq_of_parameters: t.Sequence[t.Any]
    ) -> None:
        self.await_(self._executemany(operation, seq_of_parameters))

    def setinputsizes(self, *inputsizes: t.Any) -> None:
        raise NotImplementedError


class AsyncAdapt_psqlpy_ss_cursor(
    AsyncAdapt_dbapi_ss_cursor,
    AsyncAdapt_psqlpy_cursor,
):
    """Server-side cursor implementation for psqlpy."""

    _cursor: psqlpy.Cursor | None  # type: ignore[assignment]

    def __init__(
        self, adapt_connection: "AsyncAdapt_psqlpy_connection"
    ) -> None:
        self._adapt_connection = adapt_connection
        self._connection = adapt_connection._connection
        self.await_ = adapt_connection.await_
        self._cursor = None
        self._closed = False

    def _convert_result(
        self,
        result: psqlpy.QueryResult,
    ) -> tuple[tuple[Any, ...], ...]:
        """Convert psqlpy QueryResult to tuple of tuples."""
        if result is None:
            return ()

        try:
            return tuple(
                tuple(value for _, value in row)
                for row in result.row_factory(row_factories.tuple_row)
            )
        except Exception:
            # Return empty tuple on conversion error
            return ()

    def close(self) -> None:
        """Close the cursor and release resources."""
        if self._cursor is not None and not self._closed:
            try:
                self._cursor.close()
            except Exception:
                # Ignore close errors
                pass
            finally:
                self._cursor = None
                self._closed = True

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row from the cursor."""
        if self._closed or self._cursor is None:
            return None

        try:
            result = self.await_(self._cursor.fetchone())
            converted = self._convert_result(result=result)
            return converted[0] if converted else None
        except Exception:
            return None

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch the next set of rows from the cursor."""
        if self._closed or self._cursor is None:
            return []

        try:
            if size is None:
                size = self.arraysize
            result = self.await_(self._cursor.fetchmany(size=size))
            return list(self._convert_result(result=result))
        except Exception:
            return []

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows from the cursor."""
        if self._closed or self._cursor is None:
            return []

        try:
            result = self.await_(self._cursor.fetchall())
            return list(self._convert_result(result=result))
        except Exception:
            return []

    def __iter__(self) -> t.Iterator[tuple[Any, ...]]:
        if self._closed or self._cursor is None:
            return

        iterator = self._cursor.__aiter__()
        while True:
            try:
                result = self.await_(iterator.__anext__())
                rows = self._convert_result(result=result)
                # Yield individual rows, not the entire result
                yield from rows
            except StopAsyncIteration:
                break


class AsyncAdapt_psqlpy_connection(AsyncAdapt_dbapi_connection):
    _cursor_cls = AsyncAdapt_psqlpy_cursor  # type: ignore[assignment]
    _ss_cursor_cls = AsyncAdapt_psqlpy_ss_cursor  # type: ignore[assignment]

    _connection: psqlpy.Connection  # type: ignore[assignment]
    _transaction: psqlpy.Transaction | None

    __slots__ = (
        "_invalidate_schema_cache_asof",
        "_isolation_setting",
        "_prepared_statement_cache",
        "_prepared_statement_name_func",
        "_query_cache",
        "_cache_max_size",
        "_started",
        "_transaction",
        "_connection_valid",
        "_last_ping_time",
        "_execute_mutex",
        "deferrable",
        "isolation_level",
        "readonly",
    )

    def __init__(
        self,
        dbapi: t.Any,
        connection: psqlpy.Connection,
        prepared_statement_cache_size: int = 100,
    ) -> None:
        super().__init__(dbapi, connection)  # type: ignore[arg-type]
        self.isolation_level = self._isolation_setting = None
        self.readonly = False
        self.deferrable = False
        self._transaction = None
        self._started = False
        self._connection_valid = True
        self._last_ping_time = 0.0
        self._invalidate_schema_cache_asof = time.time()

        # Async lock for coordinating concurrent operations
        self._execute_mutex = asyncio.Lock()

        # LRU cache for prepared statements. Defaults to 100 statements per
        # connection. The cache is on a per-connection basis, stored within
        # connections pooled by the connection pool.
        self._prepared_statement_cache: util.LRUCache[t.Any, t.Any] | None
        if prepared_statement_cache_size > 0:
            self._prepared_statement_cache = util.LRUCache(
                prepared_statement_cache_size
            )
        else:
            self._prepared_statement_cache = None

        # Prepared statement name function (for compatibility with asyncpg)
        self._prepared_statement_name_func = self._default_name_func

        # Legacy query cache (kept for compatibility)
        self._query_cache: dict[str, t.Any] = {}
        self._cache_max_size = prepared_statement_cache_size

    async def _check_type_cache_invalidation(
        self, invalidate_timestamp: float
    ) -> None:
        """Check if type cache needs invalidation.

        Similar to asyncpg's implementation, tracks schema changes
        that may invalidate cached type information.
        """
        if invalidate_timestamp > self._invalidate_schema_cache_asof:
            # psqlpy doesn't have reload_schema_state like asyncpg,
            # but we track the invalidation timestamp for consistency
            self._invalidate_schema_cache_asof = invalidate_timestamp

    async def _start_transaction(self) -> None:
        """Start a new transaction."""
        if self._transaction is not None:
            # Transaction already started
            return

        try:
            transaction = self._connection.transaction()
            await transaction.begin()
            self._transaction = transaction
            self._started = True
        except Exception:
            self._transaction = None
            self._started = False
            raise

    def set_isolation_level(self, level: t.Any) -> None:
        self.isolation_level = self._isolation_setting = level

    def rollback(self) -> None:
        """Rollback the current transaction."""
        if self._transaction is not None:
            try:
                await_only(self._transaction.rollback())
            except Exception:
                self._connection_valid = False
        self._transaction = None
        self._started = False

    def commit(self) -> None:
        """Commit the current transaction."""
        if self._transaction is not None:
            try:
                await_only(self._transaction.commit())
            except Exception as e:
                self._connection_valid = False
                self._transaction = None
                self._started = False
                raise e
        self._transaction = None
        self._started = False

    def is_valid(self) -> bool:
        """Check if connection is valid"""
        return self._connection_valid and self._connection is not None

    def ping(self, reconnect: t.Any = None) -> t.Any:
        """Ping the connection to check if it's alive"""
        import time

        current_time = time.time()
        # Only ping if more than 30 seconds since last ping
        if current_time - self._last_ping_time < 30:
            return self._connection_valid

        try:
            # Simple query to test connection
            await_only(self._connection.execute("SELECT 1"))
            self._connection_valid = True
            self._last_ping_time = current_time
            return True
        except Exception:
            self._connection_valid = False
            return False

    def _get_cached_query(self, query_key: str) -> t.Any | None:
        """Get a cached prepared statement if available."""
        return self._query_cache.get(query_key)

    def _cache_query(self, query_key: str, prepared_stmt: t.Any) -> None:
        """Cache a prepared statement with LRU-like eviction."""
        # Simple LRU: if cache is full, remove oldest entry
        if len(self._query_cache) >= self._cache_max_size:
            # Remove first (oldest) item
            self._query_cache.pop(next(iter(self._query_cache)))
        self._query_cache[query_key] = prepared_stmt

    def clear_query_cache(self) -> None:
        """Clear the query cache."""
        self._query_cache.clear()

    def close(self) -> None:
        self.rollback()
        self._connection.close()

    def cursor(
        self, server_side: bool = False
    ) -> AsyncAdapt_psqlpy_cursor | AsyncAdapt_psqlpy_ss_cursor:
        if server_side:
            return self._ss_cursor_cls(self)
        return self._cursor_cls(self)

    @staticmethod
    def _default_name_func() -> None:
        """Default prepared statement name function.

        Returns None to let psqlpy auto-generate statement names.
        Compatible with asyncpg's implementation.
        """
        return


# Backward compatibility aliases
PsqlpyConnection = AsyncAdapt_psqlpy_connection
PsqlpyCursor = AsyncAdapt_psqlpy_cursor
