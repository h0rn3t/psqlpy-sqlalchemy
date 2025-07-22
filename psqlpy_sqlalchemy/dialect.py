import typing as t
import uuid
from types import ModuleType
from typing import Any, Dict, MutableMapping, Sequence, Tuple

import psqlpy
from sqlalchemy import URL, util
from sqlalchemy.dialects.postgresql.base import INTERVAL, UUID, PGDialect
from sqlalchemy.dialects.postgresql.json import JSONPathType
from sqlalchemy.pool import AsyncAdaptedQueuePool, NullPool
from sqlalchemy.sql import operators, sqltypes
from sqlalchemy.sql.functions import GenericFunction

from .connection import AsyncAdapt_psqlpy_connection, PGExecutionContext_psqlpy
from .dbapi import PSQLPyAdaptDBAPI


class CompatibleNullPool(NullPool):
    """
    A NullPool wrapper that accepts but ignores pool sizing arguments.

    This class is used to maintain compatibility with middleware that passes
    pool_size and max_overflow arguments, which are not valid for NullPool
    but are commonly passed by frameworks like FastAPI with fastapi_async_sqlalchemy.
    """

    def __init__(self, creator, pool_size=None, max_overflow=None, **kw):
        # Filter out pool sizing arguments that NullPool doesn't accept
        filtered_kw = {
            k: v
            for k, v in kw.items()
            if k not in ("pool_size", "max_overflow")
        }
        super().__init__(creator, **filtered_kw)


# JSONB aggregation functions
class jsonb_agg(GenericFunction):
    """JSONB aggregation function"""

    type = sqltypes.JSON
    name = "jsonb_agg"


class jsonb_object_agg(GenericFunction):
    """JSONB object aggregation function"""

    type = sqltypes.JSON
    name = "jsonb_object_agg"


class jsonb_build_array(GenericFunction):
    """JSONB build array function"""

    type = sqltypes.JSON
    name = "jsonb_build_array"


class jsonb_build_object(GenericFunction):
    """JSONB build object function"""

    type = sqltypes.JSON
    name = "jsonb_build_object"


class jsonb_extract_path(GenericFunction):
    """JSONB extract path function"""

    type = sqltypes.JSON
    name = "jsonb_extract_path"


class jsonb_extract_path_text(GenericFunction):
    """JSONB extract path as text function"""

    type = sqltypes.Text
    name = "jsonb_extract_path_text"


class jsonb_path_exists(GenericFunction):
    """JSONB path exists function"""

    type = sqltypes.Boolean
    name = "jsonb_path_exists"


class jsonb_path_match(GenericFunction):
    """JSONB path match function"""

    type = sqltypes.Boolean
    name = "jsonb_path_match"


class jsonb_path_query(GenericFunction):
    """JSONB path query function"""

    type = sqltypes.JSON
    name = "jsonb_path_query"


class jsonb_path_query_array(GenericFunction):
    """JSONB path query array function"""

    type = sqltypes.JSON
    name = "jsonb_path_query_array"


class jsonb_path_query_first(GenericFunction):
    """JSONB path query first function"""

    type = sqltypes.JSON
    name = "jsonb_path_query_first"


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
    render_bind_cast = True


class _PGJSONB(sqltypes.JSON):
    """Enhanced JSONB type with PostgreSQL-specific operators"""

    __visit_name__ = "JSONB"
    render_bind_cast = True

    class Comparator(sqltypes.JSON.Comparator):
        """Enhanced comparator with JSONB-specific operators"""

        def contains(self, other):
            """JSONB containment operator @>"""
            return self.operate(operators.custom_op("@>"), other)

        def contained_by(self, other):
            """JSONB contained by operator <@"""
            return self.operate(operators.custom_op("<@"), other)

        def has_key(self, key):
            """JSONB has key operator ?"""
            return self.operate(operators.custom_op("?"), key)

        def has_any_key(self, keys):
            """JSONB has any key operator ?|"""
            return self.operate(operators.custom_op("?|"), keys)

        def has_all_keys(self, keys):
            """JSONB has all keys operator ?&"""
            return self.operate(operators.custom_op("?&"), keys)

        def path_exists(self, path):
            """JSONB path exists operator @?"""
            return self.operate(operators.custom_op("@?"), path)

        def path_match(self, path):
            """JSONB path match operator @@"""
            return self.operate(operators.custom_op("@@"), path)

        def concat(self, other):
            """JSONB concatenation operator ||"""
            return self.operate(operators.custom_op("||"), other)

        def delete_key(self, key):
            """JSONB delete key operator -"""
            return self.operate(operators.custom_op("-"), key)

        def delete_path(self, path):
            """JSONB delete path operator #-"""
            return self.operate(operators.custom_op("#-"), path)

    comparator_factory = Comparator


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


class _PGUUID(UUID):
    """PostgreSQL UUID type with proper parameter binding for psqlpy."""

    def bind_processor(self, dialect):
        """Process UUID parameters for psqlpy compatibility."""

        def process(value):
            if value is None:
                return None
            if isinstance(value, uuid.UUID):
                # Convert UUID objects to bytes for psqlpy
                return value.bytes
            if isinstance(value, str):
                # Validate and convert UUID strings to bytes
                try:
                    parsed_uuid = uuid.UUID(value)
                    return parsed_uuid.bytes
                except ValueError:
                    raise ValueError(f"Invalid UUID string: {value}")
            # For other types, try to convert to UUID first
            try:
                parsed_uuid = uuid.UUID(str(value))
                return parsed_uuid.bytes
            except ValueError:
                raise ValueError(f"Cannot convert {value!r} to UUID")

        return process


class PSQLPyAsyncDialect(PGDialect):
    driver = "psqlpy"
    is_async = True
    poolclass = AsyncAdaptedQueuePool

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
            sqltypes.JSON: _PGJSONB,  # Enhanced JSONB support
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
            UUID: _PGUUID,  # UUID support with proper parameter binding
            # Note: NullType mapping removed - standard PostgreSQL dialect doesn't map it
            # and mapping it with render_bind_cast=True causes DDL compilation errors
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

# Export the compatible pool class for users who need it
__all__ = ["PSQLPyAsyncDialect", "PsqlpyDialect", "CompatibleNullPool"]
