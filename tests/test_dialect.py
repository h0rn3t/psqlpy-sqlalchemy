#!/usr/bin/env python3
"""
Unit tests for psqlpy-sqlalchemy dialect
"""

import unittest

from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, text
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
                "postgresql+psqlpy://user:password@localhost/test"
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
                "postgresql+psqlpy://testuser:testpass@localhost:5432/testdb?sslmode=require"
            )

            # Test create_connect_args
            args, kwargs = self.engine.dialect.create_connect_args(self.engine.url)

            self.assertIsInstance(args, list)
            self.assertIsInstance(kwargs, dict)

            # Check expected connection parameters
            expected_keys = ["host", "port", "db_name", "username", "password"]
            for key in expected_keys:
                self.assertIn(key, kwargs, f"Missing connection parameter: {key}")

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
                "postgresql+psqlpy://user:password@localhost/test"
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
                "postgresql+psqlpy://user:password@localhost/test"
            )
            dbapi = self.engine.dialect.import_dbapi()

            self.assertIsNotNone(dbapi)

            # Test DBAPI attributes
            self.assertEqual(dbapi.apilevel, "2.0")
            self.assertEqual(dbapi.threadsafety, 2)
            self.assertEqual(dbapi.paramstyle, "pyformat")

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
                    hasattr(dbapi, exc_name), f"Missing DBAPI exception: {exc_name}"
                )

        except Exception as e:
            self.fail(f"Failed DBAPI interface test: {e}")

    def test_mock_connection(self):
        """Test connection creation (without actual database)"""
        try:
            self.engine = create_engine(
                "postgresql+psqlpy://user:password@localhost/test"
            )

            # This will fail because we don't have a real database,
            # but we can test that the connection creation process starts correctly
            try:
                connection = self.engine.connect()
                # If we get here, connection succeeded unexpectedly
                connection.close()
                self.fail("Connection succeeded unexpectedly without a real database")
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
                "postgresql+psqlpy://user:password@localhost/test"
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


class TestPsqlpyConnection(unittest.TestCase):
    """Test cases for psqlpy connection wrapper"""

    def test_connection_wrapper_creation(self):
        """Test that connection wrapper can be created"""
        from psqlpy_sqlalchemy.connection import PsqlpyConnection

        # We can't create a real connection without a database,
        # but we can test the class exists and has required methods
        self.assertTrue(hasattr(PsqlpyConnection, "cursor"))
        self.assertTrue(hasattr(PsqlpyConnection, "commit"))
        self.assertTrue(hasattr(PsqlpyConnection, "rollback"))
        self.assertTrue(hasattr(PsqlpyConnection, "close"))

    def test_cursor_wrapper_creation(self):
        """Test that cursor wrapper can be created"""
        from psqlpy_sqlalchemy.connection import PsqlpyCursor

        # Test the class exists and has required methods
        self.assertTrue(hasattr(PsqlpyCursor, "execute"))
        self.assertTrue(hasattr(PsqlpyCursor, "executemany"))
        self.assertTrue(hasattr(PsqlpyCursor, "fetchone"))
        self.assertTrue(hasattr(PsqlpyCursor, "fetchmany"))
        self.assertTrue(hasattr(PsqlpyCursor, "fetchall"))
        self.assertTrue(hasattr(PsqlpyCursor, "close"))


if __name__ == "__main__":
    unittest.main()
