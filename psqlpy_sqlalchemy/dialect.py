"""
SQLAlchemy dialect for psqlpy
"""

from typing import Any, Dict, List, Tuple

import psqlpy
from sqlalchemy.engine import default
from sqlalchemy.engine.url import URL

from .connection import PsqlpyConnection
from .dbapi import PsqlpyDBAPI


class PsqlpyDialect(default.DefaultDialect):
    """SQLAlchemy dialect for psqlpy PostgreSQL driver"""

    name = "postgresql"
    driver = "psqlpy"

    # Dialect capabilities
    supports_statement_cache = True
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

    # Connection pooling
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True

    # Transaction support
    supports_isolation_level = True
    default_isolation_level = "READ_COMMITTED"

    @classmethod
    def import_dbapi(cls):
        """Import the psqlpy module as DBAPI"""
        return PsqlpyDBAPI()

    def create_connect_args(self, url: URL) -> Tuple[List[Any], Dict[str, Any]]:
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
                    opts["ssl_mode"] = getattr(psqlpy.SslMode, value.upper(), None)
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
            # Use psqlpy.connect to create a connection
            raw_connection = psqlpy.connect(**cparams)
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
        """Set the isolation level"""
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
