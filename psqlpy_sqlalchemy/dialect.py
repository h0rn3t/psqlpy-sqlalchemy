import typing as t
from types import ModuleType
from typing import Any, Dict, MutableMapping, Sequence, Tuple

import psqlpy
from sqlalchemy import URL, util
from sqlalchemy.dialects.postgresql.base import INTERVAL, PGDialect
from sqlalchemy.dialects.postgresql.json import JSONPathType
from sqlalchemy.sql import sqltypes

from .connection import AsyncAdapt_psqlpy_connection, PGExecutionContext_psqlpy
from .dbapi import PSQLPyAdaptDBAPI


# Custom type classes with render_bind_cast for better PostgreSQL compatibility
class _PGString(sqltypes.String):
    render_bind_cast = True


class _PGJSONIntIndexType(sqltypes.JSON.JSONIntIndexType):
    __visit_name__ = "json_int_index"
    render_bind_cast = True


class _PGJSONStrIndexType(sqltypes.JSON.JSONStrIndexType):
    __visit_name__ = "json_str_index"
    render_bind_cast = True


class _PGJSONPathType(JSONPathType):
    pass


class _PGInterval(INTERVAL):
    render_bind_cast = True


class _PGTimeStamp(sqltypes.DateTime):
    render_bind_cast = True


class _PGDate(sqltypes.Date):
    render_bind_cast = True


class _PGTime(sqltypes.Time):
    render_bind_cast = True


class _PGInteger(sqltypes.Integer):
    render_bind_cast = True


class _PGSmallInteger(sqltypes.SmallInteger):
    render_bind_cast = True


class _PGBigInteger(sqltypes.BigInteger):
    render_bind_cast = True


class _PGBoolean(sqltypes.Boolean):
    render_bind_cast = True


class _PGNullType(sqltypes.NullType):
    render_bind_cast = True


class PSQLPyAsyncDialect(PGDialect):
    driver = "psqlpy"
    is_async = True

    execution_ctx_cls = PGExecutionContext_psqlpy
    supports_statement_cache = True
    supports_server_side_cursors = True
    default_paramstyle = "numeric_dollar"
    supports_sane_multi_rowcount = True

    # Additional dialect capabilities for compatibility
    supports_multivalues_insert = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_native_decimal = True
    supports_native_boolean = True
    supports_sequences = True
    sequences_optional = True
    preexecute_autoincrement_sequences = False
    postfetch_lastrowid = False
    implicit_returning = True
    full_returning = True
    insert_returning = True
    update_returning = True
    delete_returning = True
    favor_returning_over_lastrowid = True

    # Comprehensive colspecs mapping for better PostgreSQL type handling
    colspecs = util.update_copy(
        PGDialect.colspecs,
        {
            sqltypes.String: _PGString,
            sqltypes.JSON.JSONPathType: _PGJSONPathType,
            sqltypes.JSON.JSONIntIndexType: _PGJSONIntIndexType,
            sqltypes.JSON.JSONStrIndexType: _PGJSONStrIndexType,
            sqltypes.Interval: _PGInterval,
            INTERVAL: _PGInterval,
            sqltypes.Date: _PGDate,
            sqltypes.DateTime: _PGTimeStamp,
            sqltypes.Time: _PGTime,
            sqltypes.Integer: _PGInteger,
            sqltypes.SmallInteger: _PGSmallInteger,
            sqltypes.BigInteger: _PGBigInteger,
            sqltypes.Boolean: _PGBoolean,
            sqltypes.NullType: _PGNullType,
        },
    )

    @classmethod
    def import_dbapi(cls) -> ModuleType:
        return t.cast(ModuleType, PSQLPyAdaptDBAPI(__import__("psqlpy")))

    @util.memoized_property
    def _isolation_lookup(self) -> Dict[str, Any]:
        """Mapping of SQLAlchemy isolation levels to psqlpy isolation levels"""
        return {
            "READ_COMMITTED": psqlpy.IsolationLevel.ReadCommitted,
            "REPEATABLE_READ": psqlpy.IsolationLevel.RepeatableRead,
            "SERIALIZABLE": psqlpy.IsolationLevel.Serializable,
        }

    def create_connect_args(
        self,
        url: URL,
    ) -> Tuple[Sequence[str], MutableMapping[str, Any]]:
        opts = url.translate_connect_args()
        return (
            [],
            {
                "host": opts.get("host"),
                "port": opts.get("port"),
                "username": opts.get("username"),
                "db_name": opts.get("database"),
                "password": opts.get("password"),
            },
        )

    def set_isolation_level(
        self,
        dbapi_connection: AsyncAdapt_psqlpy_connection,
        level,
    ):
        dbapi_connection.set_isolation_level(self._isolation_lookup[level])

    def set_readonly(self, connection, value):
        if value is True:
            connection.readonly = psqlpy.ReadVariant.ReadOnly
        else:
            connection.readonly = psqlpy.ReadVariant.ReadWrite

    def get_readonly(self, connection):
        return connection.readonly

    def set_deferrable(self, connection, value):
        connection.deferrable = value

    def get_deferrable(self, connection):
        return connection.deferrable


dialect = PSQLPyAsyncDialect

# Backward compatibility alias for entry point system
PsqlpyDialect = PSQLPyAsyncDialect
