import asyncio
from typing import Any, Dict, List, Tuple

import psqlpy
from sqlalchemy import util
from sqlalchemy.dialects.postgresql.base import INTERVAL, PGDialect
from sqlalchemy.dialects.postgresql.json import JSONPathType
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import sqltypes

from .connection import PsqlpyConnection
from .dbapi import PsqlpyDBAPI


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


class PsqlpyDialect(PGDialect):
    """SQLAlchemy dialect for psqlpy PostgreSQL driver"""

    name = "postgresql"
    driver = "psqlpy"

    # Dialect capabilities
    supports_statement_cache = True
    supports_server_side_cursors = True
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
    default_paramstyle = "numeric_dollar"

    # Connection pooling
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True

    # Transaction support
    supports_isolation_level = True
    default_isolation_level = "READ_COMMITTED"

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
    def import_dbapi(cls):
        """Import the psqlpy module as DBAPI"""
        return PsqlpyDBAPI()

    @util.memoized_property
    def _isolation_lookup(self) -> Dict[str, Any]:
        """Mapping of SQLAlchemy isolation levels to psqlpy isolation levels"""
        return {
            "READ_COMMITTED": psqlpy.IsolationLevel.ReadCommitted,
            "REPEATABLE_READ": psqlpy.IsolationLevel.RepeatableRead,
            "SERIALIZABLE": psqlpy.IsolationLevel.Serializable,
        }

    def create_connect_args(
        self, url: URL
    ) -> Tuple[List[Any], Dict[str, Any]]:
        """Create connection arguments from SQLAlchemy URL"""
        opts = {}

        # Basic connection parameters
        if url.host:
            opts["host"] = url.host
        if url.port:
            opts["port"] = url.port
        if url.database:
            opts["db_name"] = url.database
        if url.username:
            opts["username"] = url.username
        if url.password:
            opts["password"] = url.password

        # Parse query parameters
        if url.query:
            for key, value in url.query.items():
                if isinstance(value, (list, tuple)):
                    value = value[0] if value else None

                # Map common PostgreSQL connection parameters
                if key == "sslmode":
                    opts["ssl_mode"] = getattr(
                        psqlpy.SslMode, value.upper(), None
                    )
                elif key == "application_name":
                    opts["application_name"] = value
                elif key == "connect_timeout":
                    opts["connect_timeout_sec"] = int(value)
                elif key == "options":
                    opts["options"] = value
                else:
                    # Pass through other parameters
                    opts[key] = value

        return [], opts

    def connect(self, *cargs, **cparams):
        """Create a connection to the database"""
        try:
            # psqlpy.connect returns a coroutine that needs to be awaited
            # Since SQLAlchemy dialects are synchronous, we use asyncio.run()
            connection_coro = psqlpy.connect(**cparams)
            raw_connection = asyncio.run(connection_coro)
            # Wrap it in our DBAPI-compatible connection
            return PsqlpyConnection(raw_connection)
        except Exception as e:
            # Convert psqlpy exceptions to DBAPI exceptions
            raise self._handle_exception(e)

    def do_execute(self, cursor, statement, parameters, context=None):
        """Execute a statement with parameters"""
        try:
            cursor.execute(statement, parameters)
        except Exception as e:
            raise self._handle_exception(e)

    def do_execute_no_params(self, cursor, statement, context=None):
        """Execute a statement without parameters"""
        try:
            cursor.execute(statement)
        except Exception as e:
            raise self._handle_exception(e)

    def do_executemany(self, cursor, statement, parameters, context=None):
        """Execute a statement multiple times with different parameters"""
        try:
            cursor.executemany(statement, parameters)
        except Exception as e:
            raise self._handle_exception(e)

    def is_disconnect(self, e, connection, cursor):
        """Check if an exception indicates a disconnection"""
        if isinstance(e, psqlpy.Error):
            # Check for common disconnection error patterns
            error_msg = str(e).lower()
            disconnect_patterns = [
                "connection closed",
                "connection lost",
                "server closed the connection",
                "connection reset",
                "broken pipe",
                "connection refused",
            ]
            return any(pattern in error_msg for pattern in disconnect_patterns)
        return False

    def get_isolation_level(self, dbapi_connection):
        """Get the current isolation level"""
        # psqlpy doesn't expose isolation level directly
        # We'll need to query it
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute("SHOW transaction_isolation")
            result = cursor.fetchone()
            if result:
                level = result[0].upper().replace(" ", "_")
                return level
        except Exception:
            pass
        return self.default_isolation_level

    def set_isolation_level(self, dbapi_connection, level):
        """Set the isolation level using psqlpy enums"""
        if hasattr(dbapi_connection, "set_isolation_level"):
            # Use psqlpy's native isolation level setting if available
            psqlpy_level = self._isolation_lookup.get(level)
            if psqlpy_level is not None:
                dbapi_connection.set_isolation_level(psqlpy_level)
                return

        # Fallback to SQL-based approach
        try:
            cursor = dbapi_connection.cursor()
            level_map = {
                "READ_UNCOMMITTED": "READ UNCOMMITTED",
                "READ_COMMITTED": "READ COMMITTED",
                "REPEATABLE_READ": "REPEATABLE READ",
                "SERIALIZABLE": "SERIALIZABLE",
            }
            pg_level = level_map.get(level, level)
            cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {pg_level}")
        except Exception as e:
            raise self._handle_exception(e)

    def _handle_exception(self, e):
        """Convert psqlpy exceptions to appropriate DBAPI exceptions"""
        if isinstance(e, psqlpy.Error):
            # For now, just re-raise as is
            # In a full implementation, we'd map to specific DBAPI
            # exception types
            return e
        return e

    def get_default_isolation_level(self, dbapi_conn):
        """Get the default isolation level for new connections"""
        return self.default_isolation_level

    def set_readonly(self, connection, value):
        """Set the readonly state of the connection"""
        if hasattr(connection, "readonly"):
            if value is True:
                connection.readonly = psqlpy.ReadVariant.ReadOnly
            else:
                connection.readonly = psqlpy.ReadVariant.ReadWrite
        else:
            # Fallback to SQL-based approach
            try:
                cursor = connection.cursor()
                if value:
                    cursor.execute("SET TRANSACTION READ ONLY")
                else:
                    cursor.execute("SET TRANSACTION READ WRITE")
            except Exception as e:
                raise self._handle_exception(e)

    def get_readonly(self, connection):
        """Get the readonly state of the connection"""
        if hasattr(connection, "readonly"):
            return connection.readonly == psqlpy.ReadVariant.ReadOnly
        return False

    def set_deferrable(self, connection, value):
        """Set the deferrable state of the connection"""
        if hasattr(connection, "deferrable"):
            connection.deferrable = value
        else:
            # Fallback to SQL-based approach
            try:
                cursor = connection.cursor()
                if value:
                    cursor.execute("SET TRANSACTION DEFERRABLE")
                else:
                    cursor.execute("SET TRANSACTION NOT DEFERRABLE")
            except Exception as e:
                raise self._handle_exception(e)

    def get_deferrable(self, connection):
        """Get the deferrable state of the connection"""
        if hasattr(connection, "deferrable"):
            return connection.deferrable
        return False

    def has_table(self, connection, table_name, schema=None):
        """Check if a table exists in the database"""
        if schema is None:
            schema = "public"

        query = """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_name = %s
            )
        """

        try:
            cursor = connection.cursor()
            cursor.execute(query, (schema, table_name))
            result = cursor.fetchone()
            return result[0] if result else False
        except Exception:
            # If we can't check, assume table doesn't exist
            return False
