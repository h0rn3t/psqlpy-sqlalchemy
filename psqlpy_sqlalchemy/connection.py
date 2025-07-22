import typing as t
from collections import deque
from typing import Any, Optional, Tuple

import psqlpy
from psqlpy import row_factories
from sqlalchemy.connectors.asyncio import (
    AsyncAdapt_dbapi_connection,
    AsyncAdapt_dbapi_cursor,
    AsyncAdapt_dbapi_ss_cursor,
)
from sqlalchemy.dialects.postgresql.base import PGExecutionContext
from sqlalchemy.util.concurrency import await_only

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
        "_arraysize",
        "_description",
        "_invalidate_schema_cache_asof",
        "_rowcount",
    )

    _adapt_connection: "AsyncAdapt_psqlpy_connection"
    _connection: psqlpy.Connection

    def __init__(self, adapt_connection: AsyncAdapt_dbapi_connection):
        self._adapt_connection = adapt_connection
        self._connection = adapt_connection._connection
        self._rows = deque()
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
        """Enhanced prepared statement execution with better error handling"""

        if (
            not self._adapt_connection._started
            and self._adapt_connection._transaction is None
        ):
            await self._adapt_connection._start_transaction()

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
            import re

            # Look specifically for PostgreSQL casting syntax :param::TYPE
            casting_pattern = r":([a-zA-Z_][a-zA-Z0-9_]*)::"
            casting_matches = re.findall(casting_pattern, converted_query)

            if casting_matches:
                # This is a known limitation: SQLAlchemy can't handle named parameters with explicit PostgreSQL casting
                import logging

                logging.getLogger(__name__)

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
                self._cursor = self._connection.cursor(
                    converted_query,
                    converted_params,
                )
                await self._cursor.start()
                self._rowcount = -1
                return

            results = await prepared_stmt.execute()

            rows: Tuple[Tuple[Any, ...], ...] = tuple(
                tuple(value for _, value in row)
                for row in results.row_factory(row_factories.tuple_row)
            )
            self._rows = deque(rows)
            self._rowcount = len(rows) if rows else 0

            # Track query execution statistics
            self._adapt_connection._performance_stats["queries_executed"] += 1

        except Exception:
            self._description = None
            self._rowcount = -1
            self._rows = deque()
            # Track connection errors
            self._adapt_connection._performance_stats["connection_errors"] += 1
            self._adapt_connection._connection_valid = False
            raise

    def _process_parameters(
        self,
        parameters: t.Union[
            t.Sequence[t.Any], t.Mapping[str, Any], None
        ] = None,
    ) -> t.Union[t.Sequence[t.Any], t.Mapping[str, Any], None]:
        """Process parameters to ensure proper type conversion for psqlpy."""
        if parameters is None:
            return None

        import uuid

        def process_value(value):
            """Process a single parameter value."""
            if value is None:
                return None
            if isinstance(value, uuid.UUID):
                return value.bytes
            if isinstance(value, str):
                try:
                    parsed_uuid = uuid.UUID(value)
                    return parsed_uuid.bytes
                except ValueError:
                    return value
            return value

        if isinstance(parameters, dict):
            return {
                key: process_value(value) for key, value in parameters.items()
            }
        elif isinstance(parameters, (list, tuple)):
            return [process_value(value) for value in parameters]
        else:
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
        """
        # Add debugging logging for CI troubleshooting
        import logging

        logger = logging.getLogger(__name__)

        logger.debug(
            f"Parameter conversion called - Query: {querystring!r}, "
            f"Parameters: {parameters!r}, Parameters type: {type(parameters)}"
        )

        if parameters is None or not isinstance(parameters, dict):
            logger.debug("Parameters is None or not dict, returning as-is")
            return querystring, parameters

        import re

        # Find all named parameters with optional casting syntax
        # Pattern matches :param_name optionally followed by ::TYPE
        param_pattern = r":([a-zA-Z_][a-zA-Z0-9_]*)(::[\w\[\]]+)?"

        # Find all parameter references in the query
        matches = list(re.finditer(param_pattern, querystring))

        logger.debug(f"Found {len(matches)} parameter matches in query")
        for i, match in enumerate(matches):
            logger.debug(
                f"  Match {i + 1}: '{match.group(0)}' -> param='{match.group(1)}', cast='{match.group(2)}'"
            )

        if not matches:
            logger.debug("No parameter matches found, returning as-is")
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
            # Enhanced error message with more debugging information
            available_params = list(parameters.keys()) if parameters else []
            found_params = [m.group(1) for m in matches]

            # Log additional debugging information for CI troubleshooting
            import logging

            logger = logging.getLogger(__name__)
            logger.error(
                f"Parameter conversion error - Missing parameters: {missing_params}. "
                f"Query: {querystring!r}. "
                f"Found in query: {found_params}. "
                f"Available in dict: {available_params}. "
                f"Parameters dict: {parameters!r}"
            )

            # Instead of raising an error, return the original query and parameters
            # This prevents partial conversion which can cause SQL syntax errors
            logger.warning(
                "Returning original query due to missing parameters. "
                "This may indicate a parameter processing issue."
            )
            return querystring, parameters

        # Convert the query string by replacing each parameter with its positional equivalent
        converted_query = querystring

        for i, param_name in enumerate(param_order, 1):
            # Replace all occurrences of this parameter with $N, preserving any casting
            param_pattern_specific = (
                f":({re.escape(param_name)})" + r"(::[\w\[\]]+)?"
            )
            replacement = f"${i}\\2"  # $N + casting part (group 2)

            # Perform replacement and verify it worked
            new_query = re.sub(
                param_pattern_specific, replacement, converted_query
            )

            # Defensive check: ensure replacement actually occurred
            if (
                new_query == converted_query
                and f":{param_name}" in converted_query
            ):
                raise RuntimeError(
                    f"Failed to replace parameter '{param_name}' in query. "
                    f"Pattern: {param_pattern_specific}, Query: {converted_query}"
                )

            converted_query = new_query

        # Convert parameters dict to list in the correct order
        converted_params = [
            parameters[param_name] for param_name in param_order
        ]

        # Final defensive check: ensure no named parameters remain in the converted query
        # Look for the original parameter pattern, but exclude matches that are part of casting syntax
        remaining_matches = []
        for match in re.finditer(param_pattern, converted_query):
            full_match = match.group(0)
            param_name = match.group(1)
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
                if re.search(r"\$\d+:$", preceding_text):
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

        # Log final conversion results for debugging
        logger.debug(
            f"Parameter conversion completed - "
            f"Original query: {querystring!r}, "
            f"Converted query: {converted_query!r}, "
            f"Original params: {parameters!r}, "
            f"Converted params: {converted_params!r}, "
            f"Parameter order: {param_order}"
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

    async def _executemany(
        self,
        operation: str,
        seq_of_parameters: t.Sequence[t.Sequence[t.Any]],
    ) -> None:
        adapt_connection = self._adapt_connection

        self._description = None

        if not adapt_connection._started:
            await adapt_connection._start_transaction()

        processed_seq = [
            self._process_parameters(params) for params in seq_of_parameters
        ]

        return await self._connection.execute_many(
            operation,
            processed_seq,
            True,
        )

    def execute(
        self,
        operation: t.Any,
        parameters: t.Union[
            t.Sequence[t.Any], t.Mapping[str, Any], None
        ] = None,
    ) -> None:
        await_only(self._prepare_execute(operation, parameters))

    def executemany(self, operation, seq_of_parameters) -> None:
        return await_only(self._executemany(operation, seq_of_parameters))

    def setinputsizes(self, *inputsizes):
        raise NotImplementedError


class AsyncAdapt_psqlpy_ss_cursor(
    AsyncAdapt_dbapi_ss_cursor,
    AsyncAdapt_psqlpy_cursor,
):
    """Enhanced server-side cursor with better async iteration support"""

    _cursor: psqlpy.Cursor

    def __init__(self, adapt_connection):
        self._adapt_connection = adapt_connection
        self._connection = adapt_connection._connection
        self.await_ = adapt_connection.await_
        self._cursor = None
        self._closed = False

    def _convert_result(
        self,
        result: psqlpy.QueryResult,
    ) -> Tuple[Tuple[Any, ...], ...]:
        """Enhanced result conversion with better error handling"""
        if result is None:
            return tuple()

        try:
            return tuple(
                tuple(value for _, value in row)
                for row in result.row_factory(row_factories.tuple_row)
            )
        except Exception:
            # Return empty tuple on conversion error
            return tuple()

    def close(self):
        """Enhanced close with proper state management"""
        if self._cursor is not None and not self._closed:
            try:
                self._cursor.close()
            except Exception:
                # Ignore close errors
                pass
            finally:
                self._cursor = None
                self._closed = True

    def fetchone(self):
        """Fetch one row with enhanced error handling"""
        if self._closed or self._cursor is None:
            return None

        try:
            result = self.await_(self._cursor.fetchone())
            converted = self._convert_result(result=result)
            return converted[0] if converted else None
        except Exception:
            return None

    def fetchmany(self, size=None):
        """Fetch many rows with enhanced error handling"""
        if self._closed or self._cursor is None:
            return []

        try:
            if size is None:
                size = self.arraysize
            result = self.await_(self._cursor.fetchmany(size=size))
            return list(self._convert_result(result=result))
        except Exception:
            return []

    def fetchall(self):
        """Fetch all rows with enhanced error handling"""
        if self._closed or self._cursor is None:
            return []

        try:
            result = self.await_(self._cursor.fetchall())
            return list(self._convert_result(result=result))
        except Exception:
            return []

    def __iter__(self):
        """Enhanced async iteration with better error handling"""
        if self._closed or self._cursor is None:
            return

        iterator = self._cursor.__aiter__()
        while True:
            try:
                result = self.await_(iterator.__anext__())
                rows = self._convert_result(result=result)
                if rows:
                    yield from rows
                else:
                    break
            except StopAsyncIteration:
                break
            except Exception:
                # Stop iteration on any error
                break


class AsyncAdapt_psqlpy_connection(AsyncAdapt_dbapi_connection):
    _cursor_cls = AsyncAdapt_psqlpy_cursor
    _ss_cursor_cls = AsyncAdapt_psqlpy_ss_cursor

    _connection: psqlpy.Connection

    __slots__ = (
        "_invalidate_schema_cache_asof",
        "_isolation_setting",
        "_prepared_statement_cache",
        "_prepared_statement_name_func",
        "_started",
        "_transaction",
        "_connection_valid",
        "_last_ping_time",
        "_performance_stats",
        "deferrable",
        "isolation_level",
        "readonly",
    )

    def __init__(self, dbapi, connection):
        super().__init__(dbapi, connection)
        self.isolation_level = self._isolation_setting = None
        self.readonly = False
        self.deferrable = False
        self._transaction = None
        self._started = False
        self._connection_valid = True
        self._last_ping_time = 0
        self._performance_stats = {
            "queries_executed": 0,
            "transactions_committed": 0,
            "transactions_rolled_back": 0,
            "connection_errors": 0,
        }

    async def _start_transaction(self) -> None:
        """Start a new transaction with enhanced state tracking"""
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

    def set_isolation_level(self, level):
        self.isolation_level = self._isolation_setting = level

    def rollback(self) -> None:
        """Rollback transaction with enhanced error handling"""
        try:
            if self._transaction is not None:
                await_only(self._transaction.rollback())
            else:
                await_only(self._connection.rollback())
            self._performance_stats["transactions_rolled_back"] += 1
        except Exception:
            self._performance_stats["connection_errors"] += 1
            self._connection_valid = False
            # Ignore rollback errors as connection might be in bad state
            pass
        finally:
            self._transaction = None
            self._started = False

    def commit(self) -> None:
        """Commit transaction with enhanced error handling"""
        try:
            if self._transaction is not None:
                await_only(self._transaction.commit())
            else:
                await_only(self._connection.commit())
            self._performance_stats["transactions_committed"] += 1
        except Exception as e:
            self._performance_stats["connection_errors"] += 1
            self._connection_valid = False
            # On commit failure, try to rollback
            try:
                self.rollback()
            except Exception:
                pass
            raise e
        finally:
            self._transaction = None
            self._started = False

    def is_valid(self) -> bool:
        """Check if connection is valid"""
        return self._connection_valid and self._connection is not None

    def ping(self) -> bool:
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
            self._performance_stats["connection_errors"] += 1
            return False

    def get_performance_stats(self) -> dict:
        """Get connection performance statistics"""
        return self._performance_stats.copy()

    def reset_performance_stats(self) -> None:
        """Reset performance statistics"""
        self._performance_stats = {
            "queries_executed": 0,
            "transactions_committed": 0,
            "transactions_rolled_back": 0,
            "connection_errors": 0,
        }

    def close(self):
        self.rollback()
        self._connection.close()

    def cursor(self, server_side=False):
        if server_side:
            return self._ss_cursor_cls(self)
        return self._cursor_cls(self)


# Backward compatibility aliases
PsqlpyConnection = AsyncAdapt_psqlpy_connection
PsqlpyCursor = AsyncAdapt_psqlpy_cursor
