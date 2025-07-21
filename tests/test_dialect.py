#!/usr/bin/env python3
"""
Unit tests for psqlpy-sqlalchemy dialect
"""

import unittest

from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import CreateTable


class TestPsqlpyDialect(unittest.TestCase):
    """Test cases for the psqlpy SQLAlchemy dialect"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.engine = None

    def tearDown(self):
        """Clean up after each test method."""
        if self.engine:
            self.engine.dispose()

    def test_dialect_registration(self):
        """Test that the dialect is properly registered"""
        try:
            self.engine = create_engine(
                "postgresql+psqlpy://user:password@localhost/test",
                poolclass=NullPool,
            )
            self.assertIsNotNone(self.engine.dialect)
            self.assertEqual(self.engine.dialect.name, "postgresql")
            self.assertEqual(self.engine.dialect.driver, "psqlpy")
        except Exception as e:
            self.fail(f"Failed to register dialect: {e}")

    def test_connection_string_parsing(self):
        """Test connection string parsing"""
        try:
            self.engine = create_engine(
                "postgresql+psqlpy://testuser:testpass@localhost:5432/testdb?sslmode=require",  # noqa
                poolclass=NullPool,
            )

            # Test create_connect_args
            args, kwargs = self.engine.dialect.create_connect_args(
                self.engine.url
            )

            self.assertIsInstance(args, list)
            self.assertIsInstance(kwargs, dict)

            # Check expected connection parameters
            expected_keys = ["host", "port", "db_name", "username", "password"]
            for key in expected_keys:
                self.assertIn(
                    key, kwargs, f"Missing connection parameter: {key}"
                )

            # Verify specific values
            self.assertEqual(kwargs["host"], "localhost")
            self.assertEqual(kwargs["port"], 5432)
            self.assertEqual(kwargs["db_name"], "testdb")
            self.assertEqual(kwargs["username"], "testuser")
            self.assertEqual(kwargs["password"], "testpass")

        except Exception as e:
            self.fail(f"Failed to parse connection string: {e}")

    def test_basic_sql_compilation(self):
        """Test basic SQL compilation"""
        try:
            self.engine = create_engine(
                "postgresql+psqlpy://user:password@localhost/test",
                poolclass=NullPool,
            )

            # Test basic SQL compilation
            stmt = text("SELECT 1 as test_column")
            compiled = stmt.compile(self.engine)
            self.assertIsNotNone(compiled)
            self.assertIn("SELECT 1", str(compiled))

            # Test table creation SQL
            metadata = MetaData()
            test_table = Table(
                "test_table",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String(50)),
            )

            create_ddl = CreateTable(test_table)
            create_sql = str(create_ddl.compile(self.engine))
            self.assertIsNotNone(create_sql)
            self.assertIn("CREATE TABLE test_table", create_sql)
            self.assertIn("id", create_sql)
            self.assertIn("name", create_sql)

        except Exception as e:
            self.fail(f"Failed SQL compilation: {e}")

    def test_dbapi_interface(self):
        """Test DBAPI interface"""
        try:
            self.engine = create_engine(
                "postgresql+psqlpy://user:password@localhost/test",
                poolclass=NullPool,
            )
            dbapi = self.engine.dialect.import_dbapi()

            self.assertIsNotNone(dbapi)

            # Test DBAPI attributes
            self.assertEqual(dbapi.apilevel, "2.0")
            self.assertEqual(dbapi.threadsafety, 2)
            self.assertEqual(dbapi.paramstyle, "numeric_dollar")

            # Test exception hierarchy
            exceptions = [
                "Warning",
                "Error",
                "InterfaceError",
                "DatabaseError",
                "DataError",
                "OperationalError",
                "IntegrityError",
                "InternalError",
                "ProgrammingError",
                "NotSupportedError",
            ]

            for exc_name in exceptions:
                self.assertTrue(
                    hasattr(dbapi, exc_name),
                    f"Missing DBAPI exception: {exc_name}",
                )

        except Exception as e:
            self.fail(f"Failed DBAPI interface test: {e}")

    def test_mock_connection(self):
        """Test connection creation (without actual database)"""
        try:
            self.engine = create_engine(
                "postgresql+psqlpy://user:password@localhost/test",
                poolclass=NullPool,
            )

            try:
                connection = self.engine.connect()
                # If we get here, connection succeeded unexpectedly
                connection.close()
                self.fail(
                    "Connection succeeded unexpectedly without a real database"
                )
            except Exception:
                # This is expected - we don't have a real database
                # The test passes if an exception is raised
                pass

        except Exception as e:
            # If we get here, it means the test setup itself failed
            self.fail(f"Unexpected error in connection test setup: {e}")

    def test_dialect_capabilities(self):
        """Test dialect capabilities and features"""
        try:
            self.engine = create_engine(
                "postgresql+psqlpy://user:password@localhost/test",
                poolclass=NullPool,
            )
            dialect = self.engine.dialect

            # Test key dialect capabilities
            self.assertTrue(dialect.supports_statement_cache)
            self.assertTrue(dialect.supports_multivalues_insert)
            self.assertTrue(dialect.supports_unicode_statements)
            self.assertTrue(dialect.supports_unicode_binds)
            self.assertTrue(dialect.supports_native_decimal)
            self.assertTrue(dialect.supports_native_boolean)
            self.assertTrue(dialect.supports_sequences)
            self.assertTrue(dialect.implicit_returning)
            self.assertTrue(dialect.full_returning)

        except Exception as e:
            self.fail(f"Failed dialect capabilities test: {e}")

    def test_jsonb_operators_compilation(self):
        """Test JSONB operators compile correctly"""
        try:
            self.engine = create_engine(
                "postgresql+psqlpy://user:password@localhost/test",
                poolclass=NullPool,
            )

            metadata = MetaData()
            test_table = Table(
                "test_jsonb",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("data", JSONB),
            )

            query1 = test_table.select().where(text("data @> :filter"))
            compiled1 = str(
                query1.compile(
                    dialect=self.engine.dialect,
                    compile_kwargs={"literal_binds": True},
                )
            )
            self.assertIn("@>", compiled1)

            query2 = test_table.select().where(text("data ? :key"))
            compiled2 = str(
                query2.compile(
                    dialect=self.engine.dialect,
                    compile_kwargs={"literal_binds": True},
                )
            )
            self.assertIn("?", compiled2)

            query3 = test_table.select().where(
                text("data #> :path IS NOT NULL")
            )
            compiled3 = str(
                query3.compile(
                    dialect=self.engine.dialect,
                    compile_kwargs={"literal_binds": True},
                )
            )
            self.assertIn("#>", compiled3)

        except Exception as e:
            self.fail(f"Failed JSONB operators compilation test: {e}")

    def test_jsonb_functions_compilation(self):
        """Test JSONB functions compile correctly"""
        try:
            from psqlpy_sqlalchemy.dialect import (
                jsonb_agg,
                jsonb_build_object,
            )

            self.engine = create_engine(
                "postgresql+psqlpy://user:password@localhost/test",
                poolclass=NullPool,
            )

            # Test table with JSONB column
            metadata = MetaData()
            test_table = Table(
                "test_jsonb",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("data", JSONB),
            )

            query1 = test_table.select().with_only_columns(
                jsonb_agg(test_table.c.data)
            )
            compiled1 = str(query1.compile(dialect=self.engine.dialect))
            self.assertIn("jsonb_agg", compiled1)

            query2 = test_table.select().with_only_columns(
                jsonb_build_object("key", test_table.c.id)
            )
            compiled2 = str(query2.compile(dialect=self.engine.dialect))
            self.assertIn("jsonb_build_object", compiled2)

        except Exception as e:
            self.fail(f"Failed JSONB functions compilation test: {e}")

    def test_enhanced_type_mapping(self):
        """Test enhanced type mapping with render_bind_cast"""
        try:
            from psqlpy_sqlalchemy.dialect import (
                _PGJSONB,
                _PGInteger,
                _PGString,
            )

            self.engine = create_engine(
                "postgresql+psqlpy://user:password@localhost/test",
                poolclass=NullPool,
            )

            self.assertTrue(hasattr(_PGJSONB, "render_bind_cast"))
            self.assertTrue(_PGJSONB.render_bind_cast)

            self.assertTrue(hasattr(_PGString, "render_bind_cast"))
            self.assertTrue(_PGString.render_bind_cast)

            self.assertTrue(hasattr(_PGInteger, "render_bind_cast"))
            self.assertTrue(_PGInteger.render_bind_cast)

            jsonb_type = _PGJSONB()
            comparator_class = jsonb_type.comparator_factory

            self.assertTrue(hasattr(comparator_class, "contains"))
            self.assertTrue(hasattr(comparator_class, "has_key"))
            self.assertTrue(hasattr(comparator_class, "has_any_key"))
            self.assertTrue(hasattr(comparator_class, "has_all_keys"))
            self.assertTrue(hasattr(comparator_class, "path_exists"))
            self.assertTrue(hasattr(comparator_class, "concat"))
            self.assertTrue(hasattr(comparator_class, "delete_key"))
            self.assertTrue(hasattr(comparator_class, "delete_path"))

        except Exception as e:
            self.fail(f"Failed enhanced type mapping test: {e}")

    def test_connection_performance_features(self):
        """Test connection performance monitoring features"""
        try:
            from psqlpy_sqlalchemy.connection import (
                AsyncAdapt_psqlpy_connection,
            )

            self.assertTrue(
                hasattr(AsyncAdapt_psqlpy_connection, "get_performance_stats")
            )
            self.assertTrue(
                hasattr(
                    AsyncAdapt_psqlpy_connection, "reset_performance_stats"
                )
            )
            self.assertTrue(hasattr(AsyncAdapt_psqlpy_connection, "is_valid"))
            self.assertTrue(hasattr(AsyncAdapt_psqlpy_connection, "ping"))

            expected_slots = [
                "_connection_valid",
                "_last_ping_time",
                "_performance_stats",
            ]

            for slot in expected_slots:
                self.assertIn(slot, AsyncAdapt_psqlpy_connection.__slots__)

        except Exception as e:
            self.fail(f"Failed connection performance features test: {e}")

    def test_enhanced_cursor_features(self):
        """Test enhanced cursor features"""
        try:
            from psqlpy_sqlalchemy.connection import (
                AsyncAdapt_psqlpy_ss_cursor,
            )

            cursor_methods = [
                "close",
                "fetchone",
                "fetchmany",
                "fetchall",
                "__iter__",
            ]
            for method in cursor_methods:
                self.assertTrue(hasattr(AsyncAdapt_psqlpy_ss_cursor, method))

            self.assertTrue(
                hasattr(AsyncAdapt_psqlpy_ss_cursor, "_convert_result")
            )

        except Exception as e:
            self.fail(f"Failed enhanced cursor features test: {e}")

    def test_transaction_management_features(self):
        """Test enhanced transaction management features"""
        try:
            from psqlpy_sqlalchemy.connection import (
                AsyncAdapt_psqlpy_connection,
            )

            transaction_methods = ["_start_transaction", "commit", "rollback"]
            for method in transaction_methods:
                self.assertTrue(hasattr(AsyncAdapt_psqlpy_connection, method))

            transaction_slots = ["_started", "_transaction"]
            for slot in transaction_slots:
                self.assertIn(slot, AsyncAdapt_psqlpy_connection.__slots__)

        except Exception as e:
            self.fail(f"Failed transaction management features test: {e}")


class TestPsqlpyConnection(unittest.TestCase):
    """Test cases for psqlpy connection wrapper"""

    def test_connection_wrapper_creation(self):
        """Test that connection wrapper can be created"""
        from psqlpy_sqlalchemy.connection import PsqlpyConnection

        self.assertTrue(hasattr(PsqlpyConnection, "cursor"))
        self.assertTrue(hasattr(PsqlpyConnection, "commit"))
        self.assertTrue(hasattr(PsqlpyConnection, "rollback"))
        self.assertTrue(hasattr(PsqlpyConnection, "close"))

    def test_cursor_wrapper_creation(self):
        """Test that cursor wrapper can be created"""
        from psqlpy_sqlalchemy.connection import PsqlpyCursor

        self.assertTrue(hasattr(PsqlpyCursor, "execute"))
        self.assertTrue(hasattr(PsqlpyCursor, "executemany"))
        self.assertTrue(hasattr(PsqlpyCursor, "fetchone"))
        self.assertTrue(hasattr(PsqlpyCursor, "fetchmany"))
        self.assertTrue(hasattr(PsqlpyCursor, "fetchall"))
        self.assertTrue(hasattr(PsqlpyCursor, "close"))


if __name__ == "__main__":
    unittest.main()
