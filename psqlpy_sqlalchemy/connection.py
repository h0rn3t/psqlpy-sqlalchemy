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
        if self._adapt_connection._transaction:
            await self._adapt_connection._start_transaction()

        prepared_stmt = await self._connection.prepare(
            querystring=querystring,
            parameters=parameters,
        )
        self._description = [
            (column.name, column.table_oid, None, None, None, None, None)
            for column in prepared_stmt.columns()
        ]

        if self.server_side:
            self._cursor = self._connection.cursor(
                querystring,
                parameters,
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

        return await self._connection.execute_many(
            operation,
            seq_of_parameters,
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
    _cursor: psqlpy.Cursor

    def __init__(self, adapt_connection):
        self._adapt_connection = adapt_connection
        self._connection = adapt_connection._connection
        self.await_ = adapt_connection.await_

        self._cursor = self._connection.cursor()

    def _convert_result(
        self,
        result: psqlpy.QueryResult,
    ) -> Tuple[Tuple[Any, ...], ...]:
        return tuple(
            tuple(value for _, value in row)
            for row in result.row_factory(row_factories.tuple_row)
        )

    def close(self):
        if self._cursor is not None:
            self._cursor.close()
            self._cursor = None

    def fetchone(self):
        result = self.await_(self._cursor.fetchone())
        return self._convert_result(result=result)

    def fetchmany(self, size=None):
        result = self.await_(self._cursor.fetchmany(size=size))
        return self._convert_result(result=result)

    def fetchall(self):
        result = self.await_(self._cursor.fetchall())
        return self._convert_result(result=result)

    def __iter__(self):
        iterator = self._cursor.__aiter__()
        while True:
            try:
                result = self.await_(iterator.__anext__())
                rows = self._convert_result(result=result)
                yield rows
            except StopAsyncIteration:
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

    async def _start_transaction(self) -> None:
        transaction = self._connection.transaction()
        await transaction.begin()
        self._transaction = transaction

    def set_isolation_level(self, level):
        self.isolation_level = self._isolation_setting = level

    def rollback(self) -> None:
        await_only(self._connection.rollback())
        self._transaction = None

    def commit(self) -> None:
        await_only(self._connection.commit())
        self._transaction = None

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
