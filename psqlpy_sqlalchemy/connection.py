import asyncio
import re
import time
import typing as t
import uuid
from collections import deque
from typing import Any

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

# Compiled regex patterns used for parameter substitution
_PARAM_PATTERN = re.compile(r":([a-zA-Z_][a-zA-Z0-9_]*)(::[\w\[\]]+)?")
_POSITIONAL_CHECK = re.compile(r"\$\d+:$")

# UUID pattern for validation
_UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

# Cache for compiled parameter-specific regex patterns
_PARAM_REGEX_CACHE: dict[str, re.Pattern[str]] = {}

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

        # Convert params
        converted_query, converted_params = self._convert_params_single_pass(
            querystring, parameters
        )

        try:
            # DML without RETURNING: use execute() directly
            query_upper = converted_query.upper()
            if (
                query_upper.lstrip()[:6] in ("INSERT", "UPDATE", "DELETE")
                and "RETURNING" not in query_upper
            ):
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
                self._cursor = self._connection.cursor(  # type: ignore[assignment]
                    converted_query,
                    converted_params,
                )
                await self._cursor.start()  # type: ignore[attr-defined]
                self._rowcount = -1
                return

            results = await prepared_stmt.execute()

            self._rows = deque(
                tuple(value for _, value in row)
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
        """Process parameters for type conversion (legacy, used by executemany).

        Converts UUID objects to bytes format required by psqlpy.
        """
        if parameters is None:
            return None

        def process_value(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, uuid.UUID):
                return value.bytes
            if isinstance(value, str) and _UUID_PATTERN.match(value):
                try:
                    return uuid.UUID(value).bytes
                except ValueError:
                    return value
            return value

        if isinstance(parameters, dict):
            return {k: process_value(v) for k, v in parameters.items()}
        if isinstance(parameters, list | tuple):
            return type(parameters)(process_value(v) for v in parameters)
        return process_value(parameters)

    def _convert_params_single_pass(
        self,
        querystring: str,
        parameters: t.Sequence[t.Any] | t.Mapping[str, Any] | None = None,
    ) -> tuple[str, list[Any] | None]:
        """Single-pass conversion: named→positional + UUID→bytes.

        Optimized to avoid multiple iterations over parameters.
        """
        # Fast path: no parameters
        if parameters is None:
            return querystring, None

        # Fast path: already positional (list/tuple)
        if isinstance(parameters, list | tuple):
            # Just process UUIDs
            converted: list[Any] = []
            for val in parameters:
                if isinstance(val, uuid.UUID):
                    converted.append(val.bytes)
                elif isinstance(val, str) and _UUID_PATTERN.match(val):
                    try:
                        converted.append(uuid.UUID(val).bytes)
                    except ValueError:
                        converted.append(val)
                else:
                    converted.append(val)
            return querystring, converted

        # Dict parameters: need named→positional conversion
        if not isinstance(parameters, dict):
            return querystring, None

        # Fast path: no named params in query
        if ":" not in querystring:
            return querystring, list(parameters.values())

        # Find all parameter references
        matches = list(_PARAM_PATTERN.finditer(querystring))
        if not matches:
            return querystring, list(parameters.values())

        # Build param order (first occurrence wins)
        param_order: list[str] = []
        seen: set[str] = set()
        for match in matches:
            name = match.group(1)
            if name not in seen and name in parameters:
                param_order.append(name)
                seen.add(name)

        # Check for missing params - return original if any missing
        for match in matches:
            name = match.group(1)
            if name not in parameters:
                # Missing param - return original query and values as list
                return querystring, list(parameters.values())

        # Single loop: build converted params + query replacement
        converted_params: list[Any] = []
        converted_query = querystring

        for i, name in enumerate(param_order, 1):
            val = parameters[name]
            # UUID conversion inline
            if isinstance(val, uuid.UUID):
                converted_params.append(val.bytes)
            elif isinstance(val, str) and _UUID_PATTERN.match(val):
                try:
                    converted_params.append(uuid.UUID(val).bytes)
                except ValueError:
                    converted_params.append(val)
            else:
                converted_params.append(val)

            # Get or create cached regex for this param
            if name not in _PARAM_REGEX_CACHE:
                _PARAM_REGEX_CACHE[name] = re.compile(
                    rf":({re.escape(name)})(::[\w\[\]]+)?"
                )
            converted_query = _PARAM_REGEX_CACHE[name].sub(
                f"${i}\\2", converted_query
            )

        return converted_query, converted_params

    def _convert_named_params_with_casting(
        self,
        querystring: str,
        parameters: t.Sequence[t.Any] | t.Mapping[str, Any] | None = None,
    ) -> tuple[str, t.Sequence[t.Any] | t.Mapping[str, Any] | None]:
        """Convert named parameters to positional (without UUID conversion).

        Legacy method for backward compatibility.
        """
        # Fast path: no parameters or not a dict
        if parameters is None or not isinstance(parameters, dict):
            return querystring, parameters

        # Fast path: no named params in query
        if ":" not in querystring:
            return querystring, parameters

        # Find all parameter references
        matches = list(_PARAM_PATTERN.finditer(querystring))
        if not matches:
            return querystring, parameters

        # Build param order (first occurrence wins)
        param_order: list[str] = []
        seen: set[str] = set()
        for match in matches:
            name = match.group(1)
            if name not in seen and name in parameters:
                param_order.append(name)
                seen.add(name)

        # Check for missing params
        for match in matches:
            name = match.group(1)
            if name not in parameters:
                return querystring, parameters

        # Build converted params + query
        converted_params: list[Any] = []
        converted_query = querystring

        for i, name in enumerate(param_order, 1):
            converted_params.append(parameters[name])
            if name not in _PARAM_REGEX_CACHE:
                _PARAM_REGEX_CACHE[name] = re.compile(
                    rf":({re.escape(name)})(::[\w\[\]]+)?"
                )
            converted_query = _PARAM_REGEX_CACHE[name].sub(
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

        # Fast conversion
        def convert_row(params: Any) -> list[Any]:
            if params is None:
                return []
            vals = params.values() if isinstance(params, dict) else params
            return [v.bytes if isinstance(v, uuid.UUID) else v for v in vals]

        converted_seq = [convert_row(p) for p in seq_of_parameters]

        # INSERT: multi-value optimization
        if (
            len(converted_seq) > 1
            and operation.lstrip()[:6].upper() == "INSERT"
            and "RETURNING" not in operation.upper()
        ):
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

                query = re.sub(
                    r"VALUES\s*\([^)]*\)",
                    f"VALUES {', '.join(parts)}",
                    operation,
                    flags=re.IGNORECASE,
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
