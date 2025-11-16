import asyncio
import contextlib
import re
import time
import typing as t
from collections import deque
from typing import Any, Optional, Tuple, Union

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
_CASTING_PATTERN = re.compile(r":([a-zA-Z_][a-zA-Z0-9_]*)::")
_POSITIONAL_CHECK = re.compile(r"\$\d+:$")

# UUID pattern for validation
_UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

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
    _cursor: t.Optional[t.Any]  # type: ignore[assignment]
    _awaitable_cursor_close: bool = False

    def __init__(
        self, adapt_connection: "AsyncAdapt_psqlpy_connection"
    ) -> None:
        self._adapt_connection = adapt_connection
        self._connection = adapt_connection._connection
        self.await_ = adapt_connection.await_
        self._rows: deque[t.Any] = deque()
        self._cursor = None
        self._description: t.Optional[t.List[t.Tuple[t.Any, ...]]] = None
        self._arraysize = 1
        self._rowcount = -1
        self._invalidate_schema_cache_asof = 0

    async def _prepare_execute(
        self,
        querystring: str,
        parameters: t.Union[
            t.Sequence[t.Any], t.Mapping[str, Any], None
        ] = None,
    ) -> None:
        """Execute a prepared statement.

        Ensures transaction context is active before executing the statement.
        Processes parameters to handle type conversions and named-to-positional
        parameter conversion for PostgreSQL's numeric parameter style.
        """
        adapt_conn = self._adapt_connection

        # Ensure transaction is started
        if not adapt_conn._started:
            await adapt_conn._start_transaction()

        # Process parameters to ensure proper type conversion (especially for UUIDs)
        processed_parameters = self._process_parameters(parameters)

        # Convert named parameters with casting syntax to positional parameters
        converted_query, converted_params = (
            self._convert_named_params_with_casting(
                querystring, processed_parameters
            )
        )

        # Handle mixed parameter styles specifically for explicit PostgreSQL casting
        # Only trigger this for queries with explicit casting syntax like :param::TYPE
        if (
            converted_params is not None
            and not isinstance(converted_params, dict)
            and converted_query == querystring
        ):  # Query unchanged means mixed parameters detected
            # Look specifically for PostgreSQL casting syntax :param::TYPE
            casting_pattern = r":([a-zA-Z_][a-zA-Z0-9_]*)::"
            casting_matches = re.findall(casting_pattern, converted_query)

            if casting_matches:
                # This is a known limitation: SQLAlchemy can't handle named parameters with explicit PostgreSQL casting
                raise RuntimeError(
                    f"Named parameters with explicit PostgreSQL casting are not supported. "
                    f"Found casting parameters: {casting_matches} in query: {converted_query[:100]}... "
                    f"SQLAlchemy filters out parameters when explicit casting syntax like ':param::TYPE' is used. "
                    f"Solutions: "
                    f"1) Use positional parameters: 'WHERE uid = $1::UUID LIMIT $2' with parameters as a list, "
                    f"2) Remove explicit casting: 'WHERE uid = :uid LIMIT :limit' (casting will be handled automatically), "
                    f"3) Use SQLAlchemy's cast() function: 'WHERE uid = cast(:uid, UUID) LIMIT :limit'"
                )

        try:
            # NOTE: psqlpy's Python API requires parameters at prepare() time
            # and PreparedStatement.execute() doesn't accept parameters.
            # While psqlpy's internal Rust API supports reusable prepared statements
            # (used by execute_many), the Python API doesn't expose this capability.
            # This prevents caching prepared statements like asyncpg does.
            prepared_stmt = await self._connection.prepare(
                querystring=converted_query,
                parameters=converted_params,
            )

            self._description = [
                (
                    column.name,
                    column.table_oid,
                    None,  # display_size
                    None,  # internal_size
                    None,  # precision
                    None,  # scale
                    None,  # null_ok
                )
                for column in prepared_stmt.columns()
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

            # Direct iteration without intermediate tuple creation
            rows_list = [
                tuple(value for _, value in row)
                for row in results.row_factory(row_factories.tuple_row)
            ]
            self._rows = deque(rows_list)
            self._rowcount = len(rows_list)

        except Exception:
            self._description = None
            self._rowcount = -1
            self._rows = deque()
            self._adapt_connection._connection_valid = False
            raise

    def _process_parameters(
        self,
        parameters: t.Union[
            t.Sequence[t.Any], t.Mapping[str, Any], None
        ] = None,
    ) -> t.Union[t.Sequence[t.Any], t.Mapping[str, Any], None]:
        """Process parameters for type conversion.

        Converts UUID objects to bytes format required by psqlpy.
        Also handles string UUIDs by parsing and converting to bytes.
        """
        if parameters is None:
            return None

        import uuid

        def process_value(value: Any) -> Any:
            """Process a single parameter value for UUID conversion."""
            if value is None:
                return None
            if isinstance(value, uuid.UUID):
                return value.bytes
            # Only attempt UUID parsing for strings matching UUID pattern
            if isinstance(value, str) and _UUID_PATTERN.match(value):
                try:
                    parsed_uuid = uuid.UUID(value)
                    return parsed_uuid.bytes
                except ValueError:
                    # Shouldn't happen with valid pattern, but be safe
                    return value
            return value

        if isinstance(parameters, dict):
            return {
                key: process_value(value) for key, value in parameters.items()
            }
        if isinstance(parameters, (list, tuple)):
            return type(parameters)(
                process_value(value) for value in parameters
            )
        return process_value(parameters)

    def _convert_named_params_with_casting(
        self,
        querystring: str,
        parameters: t.Union[
            t.Sequence[t.Any], t.Mapping[str, Any], None
        ] = None,
    ) -> t.Tuple[str, t.Union[t.Sequence[t.Any], t.Mapping[str, Any], None]]:
        """Convert named parameters with PostgreSQL casting syntax to positional parameters.

        Transforms queries like:
        'SELECT * FROM table WHERE col = :param::UUID LIMIT :limit'

        To:
        'SELECT * FROM table WHERE col = $1::UUID LIMIT $2'

        And converts the parameters dict to a list in the correct order.
        Uses pre-compiled regex patterns for parameter detection.
        """
        if parameters is None or not isinstance(parameters, dict):
            return querystring, parameters

        # Find all parameter references in the query using pre-compiled pattern
        matches = list(_PARAM_PATTERN.finditer(querystring))

        if not matches:
            return querystring, parameters

        # Build the conversion mapping and new parameter list
        param_order = []
        seen_params = set()
        missing_params = []

        # Process matches to determine parameter order (first occurrence wins)
        for match in matches:
            param_name = match.group(1)
            if param_name not in seen_params:
                if param_name in parameters:
                    param_order.append(param_name)
                    seen_params.add(param_name)
                else:
                    missing_params.append(param_name)

        # Defensive check: ensure all parameters found in query are available
        if missing_params:
            # Instead of raising an error, return the original query and parameters
            # This prevents partial conversion which can cause SQL syntax errors
            return querystring, parameters

        # Convert the query string by replacing each parameter with its positional equivalent
        converted_query = querystring

        for i, param_name in enumerate(param_order, 1):
            # Replace all occurrences of this parameter with $N, preserving any casting
            param_pattern_specific = re.compile(
                f":({re.escape(param_name)})" + r"(::[\w\[\]]+)?"
            )
            replacement = f"${i}\\2"  # $N + casting part (group 2)

            # Perform replacement and verify it worked
            new_query = param_pattern_specific.sub(
                replacement, converted_query
            )

            # Defensive check: ensure replacement actually occurred
            if (
                new_query == converted_query
                and f":{param_name}" in converted_query
            ):
                raise RuntimeError(
                    f"Failed to replace parameter '{param_name}' in query. "
                    f"Query: {converted_query}"
                )

            converted_query = new_query

        # Convert parameters dict to list in the correct order
        converted_params = [
            parameters[param_name] for param_name in param_order
        ]

        # Final defensive check: ensure no named parameters remain in the converted query
        # Look for the original parameter pattern, but exclude matches that are part of casting syntax
        remaining_matches = []
        for match in _PARAM_PATTERN.finditer(converted_query):
            full_match = match.group(0)
            # Check if this looks like a real parameter (not casting syntax)
            # Real parameters should not be preceded by a positional parameter like $1, $2, etc.
            start_pos = match.start()
            if start_pos > 0:
                # Look at the characters before the match to see if this is casting syntax
                # For casting syntax like $1::UUID, we need to check if preceded by $N:
                preceding_text = converted_query[
                    max(0, start_pos - 4) : start_pos
                ]
                # If preceded by $N: (positional parameter followed by colon), this is casting syntax
                if _POSITIONAL_CHECK.search(preceding_text):
                    continue
                # Also check the older pattern for backward compatibility
                if re.search(r"\$\d+$", preceding_text):
                    continue
            remaining_matches.append(full_match)

        if remaining_matches:
            raise RuntimeError(
                f"Conversion incomplete: named parameters still present in query: {remaining_matches}. "
                f"Converted query: {converted_query}, Original query: {querystring}"
            )

        return converted_query, converted_params

    @property
    def description(self) -> "Optional[_DBAPICursorDescription]":
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

    def _is_simple_insert(self, operation: str) -> bool:
        """Check if operation is a simple INSERT statement.

        Returns True if the query is INSERT INTO with VALUES clause
        and no RETURNING clause, allowing multi-value INSERT transformation.
        """
        operation_upper = operation.upper().strip()
        return (
            operation_upper.startswith("INSERT INTO")
            and "VALUES" in operation_upper
            and "RETURNING" not in operation_upper
        )

    async def _executemany(
        self,
        operation: str,
        seq_of_parameters: t.Sequence[t.Sequence[t.Any]],
    ) -> None:
        """Execute a batch of parameter sets.

        For simple INSERT statements, automatically transforms multiple
        individual INSERTs into a single multi-value INSERT statement:

            INSERT INTO t VALUES ($1, $2)  (executed N times)

        Becomes:

            INSERT INTO t VALUES ($1,$2), ($3,$4), ..., ($N*2-1,$N*2)

        This transformation reduces network round-trips from N to 1.
        For non-INSERT statements, delegates to psqlpy's execute_many.
        """
        adapt_connection = self._adapt_connection
        self._description = None

        # Check for schema cache invalidation
        await adapt_connection._check_type_cache_invalidation(
            self._invalidate_schema_cache_asof
        )

        # Ensure transaction context is active before batch execution
        if not adapt_connection._started:
            await adapt_connection._start_transaction()

        # Process all parameters first
        if seq_of_parameters and all(
            isinstance(p, (list, tuple)) for p in seq_of_parameters
        ):
            converted_seq = [list(p) for p in seq_of_parameters]
        else:
            converted_seq = []
            for params in seq_of_parameters:
                processed = self._process_parameters(params)
                if processed is None:
                    converted_seq.append([])
                elif isinstance(processed, dict):
                    converted_seq.append(list(processed.values()))
                elif isinstance(processed, (list, tuple)):
                    converted_seq.append(list(processed))
                else:
                    converted_seq.append([processed])

        # For simple INSERT statements, transform to multi-value INSERT
        # to reduce network round-trips
        if self._is_simple_insert(operation) and len(converted_seq) > 1:
            # Build multi-value INSERT:  VALUES ($1,$2), ($3,$4), ...
            # Count placeholders in original query
            placeholder_count = operation.count("$")

            if placeholder_count > 0:
                # Build new VALUES clause with all rows
                values_parts = []
                flat_params = []
                param_idx = 1

                for row_params in converted_seq:
                    # Create placeholders for this row: ($1, $2, ...)
                    row_placeholders = ", ".join(
                        [
                            f"${i}"
                            for i in range(
                                param_idx, param_idx + len(row_params)
                            )
                        ]
                    )
                    values_parts.append(f"({row_placeholders})")
                    flat_params.extend(row_params)
                    param_idx += len(row_params)

                # Replace original VALUES (...) with multi-row VALUES
                # Find and replace the VALUES clause
                import re

                multi_value_query = re.sub(
                    r"VALUES\s*\([^)]*\)",
                    f"VALUES {', '.join(values_parts)}",
                    operation,
                    flags=re.IGNORECASE,
                )

                # Execute as single query
                try:
                    await self._connection.execute(
                        multi_value_query, flat_params
                    )
                    return None
                except Exception:
                    # If multi-value fails, fall back to execute_many
                    pass

        # For non-INSERT statements, use pipeline when transaction is active.
        # This provides protocol-level batching similar to asyncpg.executemany().
        # Pipeline sends all queries together and waits for all responses,
        # dramatically reducing network round-trips compared to execute_many.
        if adapt_connection._transaction is not None:
            try:
                # Build queries list for pipeline: [(query, params), ...]
                queries: t.List[t.Tuple[str, t.Optional[t.List[t.Any]]]] = [
                    (operation, params) for params in converted_seq
                ]
                await adapt_connection._transaction.pipeline(
                    queries, prepared=True
                )
                return None
            except Exception:
                # If pipeline fails, fall back to execute_many
                pass

        # Fallback: use standard execute_many with prepared statements
        return await self._connection.execute_many(
            operation,
            converted_seq,
            prepared=True,
        )

    def execute(
        self,
        operation: t.Any,
        parameters: t.Union[
            t.Sequence[t.Any], t.Mapping[str, Any], None
        ] = None,
    ) -> None:
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

    _cursor: t.Optional[psqlpy.Cursor]  # type: ignore[assignment]

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
    ) -> Tuple[Tuple[Any, ...], ...]:
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

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        """Fetch the next row from the cursor."""
        if self._closed or self._cursor is None:
            return None

        try:
            result = self.await_(self._cursor.fetchone())
            converted = self._convert_result(result=result)
            return converted[0] if converted else None
        except Exception:
            return None

    def fetchmany(self, size: Optional[int] = None) -> t.List[Tuple[Any, ...]]:
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

    def fetchall(self) -> t.List[Tuple[Any, ...]]:
        """Fetch all remaining rows from the cursor."""
        if self._closed or self._cursor is None:
            return []

        try:
            result = self.await_(self._cursor.fetchall())
            return list(self._convert_result(result=result))
        except Exception:
            return []

    def __iter__(self) -> t.Iterator[Tuple[Any, ...]]:
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
    _transaction: t.Optional[psqlpy.Transaction]

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
        self._prepared_statement_cache: t.Optional[util.LRUCache[t.Any, t.Any]]
        if prepared_statement_cache_size > 0:
            self._prepared_statement_cache = util.LRUCache(
                prepared_statement_cache_size
            )
        else:
            self._prepared_statement_cache = None

        # Prepared statement name function (for compatibility with asyncpg)
        self._prepared_statement_name_func = self._default_name_func

        # Legacy query cache (kept for compatibility)
        self._query_cache: t.Dict[str, t.Any] = {}
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
        try:
            if self._transaction is not None:
                await_only(self._transaction.rollback())
            else:
                await_only(self._connection.rollback())  # type: ignore[attr-defined]
        except Exception:
            self._connection_valid = False
            # Ignore rollback errors as connection might be in bad state
            pass
        finally:
            self._transaction = None
            self._started = False

    def commit(self) -> None:
        """Commit the current transaction."""
        try:
            if self._transaction is not None:
                await_only(self._transaction.commit())
            else:
                await_only(self._connection.commit())  # type: ignore[attr-defined]
        except Exception as e:
            self._connection_valid = False
            # On commit failure, try to rollback
            with contextlib.suppress(Exception):
                self.rollback()
            raise e
        finally:
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

    def _get_cached_query(self, query_key: str) -> t.Optional[t.Any]:
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
    ) -> Union[AsyncAdapt_psqlpy_cursor, AsyncAdapt_psqlpy_ss_cursor]:
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
