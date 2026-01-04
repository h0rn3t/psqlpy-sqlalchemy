import re
import sys
import time
import typing as t
import uuid
from collections import deque
from functools import lru_cache
from typing import Any, Final

import psqlpy
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

# Compiled regex patterns
_PARAM_PATTERN: Final = re.compile(r":([a-zA-Z_][a-zA-Z0-9_]*)(::[\w\[\]]+)?")
_VALUES_PATTERN: Final = re.compile(r"VALUES\s*\([^)]*\)", re.IGNORECASE)
# Keep UUID pattern for backward compatibility (tests import it)
_UUID_PATTERN: Final = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

# DML keywords as frozenset for O(1) lookup
_DML_KEYWORDS: Final[frozenset[str]] = frozenset(
    ("INSERT", "UPDATE", "DELETE")
)

# Pre-compute UUID class for faster comparison
_UUID_CLASS: Final = uuid.UUID

# Empty tuple/list constants to avoid allocations
_EMPTY_TUPLE: Final[tuple[()]] = ()
_EMPTY_DEQUE: Final[deque[t.Any]] = deque()


@lru_cache(maxsize=256)
def _get_param_regex(name: str) -> re.Pattern[str]:
    """Cached regex pattern for parameter substitution."""
    return re.compile(rf":({re.escape(name)})(::[\w\[\]]+)?")


@lru_cache(maxsize=1024)
def _analyze_query(query: str) -> tuple[bool, bool, str]:
    """Cache query analysis: (is_dml_without_returning, has_named_params, uppercase).

    Caching query analysis avoids repeated string operations for the same queries.
    """
    q_upper = query.upper()
    start = q_upper.lstrip()[:6]
    is_dml = start in _DML_KEYWORDS and "RETURNING" not in q_upper
    has_colon = ":" in query
    return is_dml, has_colon, q_upper


def _check_dml(query: str) -> tuple[bool, str]:
    """Check if query is DML and return uppercase version.

    Backward compatibility wrapper around _analyze_query.
    """
    is_dml, _, q_upper = _analyze_query(query)
    return is_dml, q_upper


def _convert_uuid(val: t.Any) -> t.Any:
    """Convert UUID strings to UUID objects for psqlpy binary protocol.

    Optimized: uses type() instead of isinstance() and length checks
    instead of regex for faster non-UUID path.
    """
    # Fast path: already UUID
    if type(val) is _UUID_CLASS:
        return val
    # Check string with length-based UUID detection (no regex)
    # UUID format: 8-4-4-4-12 = 36 chars with 4 dashes
    if (
        type(val) is str
        and len(val) == 36
        and val[8] == "-"
        and val[13] == "-"
    ):
        try:
            return _UUID_CLASS(val)
        except ValueError:
            pass
    return val


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
        """Execute a query with optimized paths for DML and SELECT."""
        if not self._adapt_connection._started:
            await self._adapt_connection._start_transaction()

        converted_query, converted_params = self._convert_params(
            querystring, parameters
        )

        try:
            # Use cached query analysis
            is_dml, _, _ = _analyze_query(converted_query)

            if is_dml:
                # DML without RETURNING: use execute() directly
                await self._connection.execute(
                    converted_query, converted_params, prepared=True
                )
                self._description = None
                self._rowcount = 1
                self._rows = deque()
                return

            # Server-side cursor path
            if self.server_side:
                self._cursor = self._connection.cursor(
                    converted_query,
                    converted_params,
                )
                await self._cursor.start()
                self._rowcount = -1
                self._description = None
                return

            # SELECT: use fetch() for single round-trip
            result = await self._connection.fetch(
                converted_query, converted_params, prepared=True
            )

            # Get raw dict results and convert to tuples
            raw_rows = result.result()

            if raw_rows:
                # Build description from first row's keys
                first_row = raw_rows[0]
                self._description = [
                    (key, None, None, None, None, None, None)
                    for key in first_row
                ]
                # Convert dict rows to value tuples
                self._rows = deque(tuple(row.values()) for row in raw_rows)
                self._rowcount = len(raw_rows)
            else:
                self._description = []
                self._rows = deque()
                self._rowcount = 0

        except Exception:
            self._description = None
            self._rowcount = -1
            self._rows = deque()
            self._adapt_connection._connection_valid = False
            raise

    def _convert_params(
        self,
        querystring: str,
        parameters: t.Sequence[t.Any] | t.Mapping[str, Any] | None = None,
    ) -> tuple[str, list[Any] | None]:
        """Convert parameters: named→positional + UUID handling.

        Optimized with early exits and minimal allocations.
        """
        if parameters is None:
            return querystring, None

        # Fast path: already positional (list/tuple)
        if isinstance(parameters, list | tuple):
            # Only convert if non-empty
            if not parameters:
                return querystring, []
            return querystring, [_convert_uuid(v) for v in parameters]

        # Dict parameters: need named→positional conversion
        if not isinstance(parameters, dict):
            return querystring, None

        # Check for named params using cached analysis
        _, has_colon, _ = _analyze_query(querystring)
        if not has_colon:
            # No named params possible
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

    def _process_parameters(
        self,
        parameters: t.Sequence[t.Any] | t.Mapping[str, Any] | None = None,
    ) -> t.Sequence[t.Any] | t.Mapping[str, Any] | None:
        """Process parameters for type conversion (legacy compatibility)."""
        if parameters is None:
            return None
        return parameters

    def _convert_named_params_with_casting(
        self,
        querystring: str,
        parameters: t.Sequence[t.Any] | t.Mapping[str, Any] | None = None,
    ) -> tuple[str, t.Sequence[t.Any] | t.Mapping[str, Any] | None]:
        """Convert named parameters to positional (legacy compatibility)."""
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

    # Alias for backward compatibility
    _convert_params_single_pass = _convert_params

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
        """Execute a batch of parameter sets with multi-value INSERT optimization."""
        if not self._adapt_connection._started:
            await self._adapt_connection._start_transaction()

        # Fast conversion
        converted_seq = [
            [
                _convert_uuid(v)
                for v in (p.values() if isinstance(p, dict) else p or [])
            ]
            for p in seq_of_parameters
        ]

        # Use cached query analysis
        _, _, q_upper = _analyze_query(operation)

        # INSERT: multi-value optimization for batches > 1
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
        # Auto-detect batch operations for better performance
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
            return _EMPTY_TUPLE

        try:
            # Try row_factory first for tuple_row format compatibility
            if hasattr(result, "row_factory"):
                from psqlpy import row_factories

                rows = result.row_factory(row_factories.tuple_row)
                if rows:
                    # Check if it's the (name, value) tuple format
                    first = rows[0]
                    if (
                        first
                        and isinstance(first[0], tuple)
                        and len(first[0]) == 2
                    ):
                        return tuple(
                            tuple(value for _, value in row) for row in rows
                        )

            # Fallback to result() which returns list of dicts
            if hasattr(result, "result"):
                raw = result.result()
                if raw and isinstance(raw[0], dict):
                    return tuple(tuple(row.values()) for row in raw)

            return _EMPTY_TUPLE
        except Exception:
            return _EMPTY_TUPLE

    def close(self) -> None:
        """Close the cursor and release resources."""
        if self._cursor is not None and not self._closed:
            try:
                self._cursor.close()
            except Exception:
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

        # LRU cache for prepared statements
        self._prepared_statement_cache: util.LRUCache[t.Any, t.Any] | None
        if prepared_statement_cache_size > 0:
            self._prepared_statement_cache = util.LRUCache(
                prepared_statement_cache_size
            )
        else:
            self._prepared_statement_cache = None

        self._prepared_statement_name_func = self._default_name_func
        self._query_cache: dict[str, t.Any] = {}
        self._cache_max_size = prepared_statement_cache_size

    async def _check_type_cache_invalidation(
        self, invalidate_timestamp: float
    ) -> None:
        """Check if type cache needs invalidation."""
        if invalidate_timestamp > self._invalidate_schema_cache_asof:
            self._invalidate_schema_cache_asof = invalidate_timestamp

    async def _start_transaction(self) -> None:
        """Start a new transaction."""
        if self._transaction is not None:
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
        current_time = time.time()
        # Only ping if more than 30 seconds since last ping
        if current_time - self._last_ping_time < 30:
            return self._connection_valid

        try:
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
        if len(self._query_cache) >= self._cache_max_size:
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
        """Default prepared statement name function."""
        return


# Backward compatibility aliases
PsqlpyConnection = AsyncAdapt_psqlpy_connection
PsqlpyCursor = AsyncAdapt_psqlpy_cursor
