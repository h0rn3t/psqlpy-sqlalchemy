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

            self.assertTrue(hasattr(AsyncAdapt_psqlpy_connection, "is_valid"))
            self.assertTrue(hasattr(AsyncAdapt_psqlpy_connection, "ping"))

            expected_slots = [
                "_connection_valid",
                "_last_ping_time",
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


class TestJSONBOperators(unittest.TestCase):
    """Test cases for JSONB operators"""

    def setUp(self):
        """Set up test fixtures"""
        from psqlpy_sqlalchemy.dialect import _PGJSONB

        self.jsonb_type = _PGJSONB()

    def test_jsonb_contains_operator(self):
        """Test JSONB contains operator @>"""
        from sqlalchemy import Column, MetaData, Table

        metadata = MetaData()
        test_table = Table("test", metadata, Column("data", self.jsonb_type))
        col = test_table.c.data

        # Test contains operator directly through comparator
        comparator = col.comparator
        expr = comparator.contains({"key": "value"})
        self.assertIsNotNone(expr)

    def test_jsonb_contained_by_operator(self):
        """Test JSONB contained by operator <@"""
        from sqlalchemy import Column, MetaData, Table

        metadata = MetaData()
        test_table = Table("test", metadata, Column("data", self.jsonb_type))
        col = test_table.c.data

        # Test contained_by operator
        expr = col.contained_by({"key": "value"})
        self.assertIsNotNone(expr)

    def test_jsonb_has_key_operator(self):
        """Test JSONB has key operator ?"""
        from sqlalchemy import Column, MetaData, Table

        metadata = MetaData()
        test_table = Table("test", metadata, Column("data", self.jsonb_type))
        col = test_table.c.data

        # Test has_key operator
        expr = col.has_key("test_key")
        self.assertIsNotNone(expr)

    def test_jsonb_has_any_key_operator(self):
        """Test JSONB has any key operator ?|"""
        from sqlalchemy import Column, MetaData, Table

        metadata = MetaData()
        test_table = Table("test", metadata, Column("data", self.jsonb_type))
        col = test_table.c.data

        # Test has_any_key operator
        expr = col.has_any_key(["key1", "key2"])
        self.assertIsNotNone(expr)

    def test_jsonb_has_all_keys_operator(self):
        """Test JSONB has all keys operator ?&"""
        from sqlalchemy import Column, MetaData, Table

        metadata = MetaData()
        test_table = Table("test", metadata, Column("data", self.jsonb_type))
        col = test_table.c.data

        # Test has_all_keys operator
        expr = col.has_all_keys(["key1", "key2"])
        self.assertIsNotNone(expr)

    def test_jsonb_path_exists_operator(self):
        """Test JSONB path exists operator @?"""
        from sqlalchemy import Column, MetaData, Table

        metadata = MetaData()
        test_table = Table("test", metadata, Column("data", self.jsonb_type))
        col = test_table.c.data

        # Test path_exists operator
        expr = col.path_exists("$.key")
        self.assertIsNotNone(expr)

    def test_jsonb_path_match_operator(self):
        """Test JSONB path match operator @@"""
        from sqlalchemy import Column, MetaData, Table

        metadata = MetaData()
        test_table = Table("test", metadata, Column("data", self.jsonb_type))
        col = test_table.c.data

        # Test path_match operator
        expr = col.path_match('$.key == "value"')
        self.assertIsNotNone(expr)

    def test_jsonb_concat_operator(self):
        """Test JSONB concatenation operator ||"""
        from sqlalchemy import Column, MetaData, Table

        metadata = MetaData()
        test_table = Table("test", metadata, Column("data", self.jsonb_type))
        col = test_table.c.data

        # Test concat operator
        expr = col.concat({"new_key": "new_value"})
        self.assertIsNotNone(expr)

    def test_jsonb_delete_key_operator(self):
        """Test JSONB delete key operator -"""
        from sqlalchemy import Column, MetaData, Table

        metadata = MetaData()
        test_table = Table("test", metadata, Column("data", self.jsonb_type))
        col = test_table.c.data

        # Test delete_key operator
        expr = col.delete_key("unwanted_key")
        self.assertIsNotNone(expr)

    def test_jsonb_delete_path_operator(self):
        """Test JSONB delete path operator #-"""
        from sqlalchemy import Column, MetaData, Table

        metadata = MetaData()
        test_table = Table("test", metadata, Column("data", self.jsonb_type))
        col = test_table.c.data

        # Test delete_path operator
        expr = col.delete_path(["path", "to", "key"])
        self.assertIsNotNone(expr)


class TestUUIDBindProcessor(unittest.TestCase):
    """Test cases for UUID bind processor"""

    def setUp(self):
        """Set up test fixtures"""
        from psqlpy_sqlalchemy.dialect import _PGUUID, PSQLPyAsyncDialect

        self.uuid_type = _PGUUID()
        self.dialect = PSQLPyAsyncDialect()

    def test_uuid_bind_processor_with_uuid_object(self):
        """Test UUID bind processor with UUID object"""
        import uuid

        processor = self.uuid_type.bind_processor(self.dialect)
        test_uuid = uuid.uuid4()

        result = processor(test_uuid)
        self.assertEqual(result, test_uuid.bytes)

    def test_uuid_bind_processor_with_uuid_string(self):
        """Test UUID bind processor with UUID string"""
        import uuid

        processor = self.uuid_type.bind_processor(self.dialect)
        test_uuid = uuid.uuid4()
        test_uuid_str = str(test_uuid)

        result = processor(test_uuid_str)
        self.assertEqual(result, test_uuid.bytes)

    def test_uuid_bind_processor_with_none(self):
        """Test UUID bind processor with None"""
        processor = self.uuid_type.bind_processor(self.dialect)

        result = processor(None)
        self.assertIsNone(result)

    def test_uuid_bind_processor_with_invalid_string(self):
        """Test UUID bind processor with invalid string"""
        processor = self.uuid_type.bind_processor(self.dialect)

        with self.assertRaises(ValueError) as cm:
            processor("invalid-uuid-string")

        self.assertIn("Invalid UUID string", str(cm.exception))

    def test_uuid_bind_processor_with_convertible_value(self):
        """Test UUID bind processor with convertible value"""
        import uuid

        processor = self.uuid_type.bind_processor(self.dialect)
        test_uuid = uuid.uuid4()

        # Test with a value that can be converted to UUID
        result = processor(str(test_uuid))
        self.assertEqual(result, test_uuid.bytes)

    def test_uuid_bind_processor_with_invalid_value(self):
        """Test UUID bind processor with invalid value"""
        processor = self.uuid_type.bind_processor(self.dialect)

        with self.assertRaises(ValueError) as cm:
            processor(12345)  # Invalid value that can't be converted to UUID

        self.assertIn("Cannot convert", str(cm.exception))

    def test_uuid_bind_processor_with_non_string_convertible(self):
        """Test UUID bind processor with non-string value that can be converted"""
        import uuid

        processor = self.uuid_type.bind_processor(self.dialect)
        test_uuid = uuid.uuid4()

        # Test with a custom object that has __str__ method
        class CustomUUID:
            def __str__(self):
                return str(test_uuid)

        custom_obj = CustomUUID()
        result = processor(custom_obj)
        self.assertEqual(result, test_uuid.bytes)


class TestDialectMethods(unittest.TestCase):
    """Test cases for additional dialect methods"""

    def setUp(self):
        """Set up test fixtures"""
        from psqlpy_sqlalchemy.dialect import PSQLPyAsyncDialect

        self.dialect = PSQLPyAsyncDialect()

    def test_dialect_initialization(self):
        """Test dialect initialization"""
        self.assertEqual(self.dialect.driver, "psqlpy")
        self.assertTrue(self.dialect.is_async)
        self.assertIsNotNone(self.dialect.poolclass)

    def test_dialect_dbapi_property(self):
        """Test dialect dbapi property"""
        # This should trigger the dbapi property getter
        # The dbapi property may return None if psqlpy is not available
        dbapi = self.dialect.dbapi
        # Just test that accessing the property doesn't raise an error
        # The actual value depends on whether psqlpy is available
        self.assertTrue(dbapi is None or hasattr(dbapi, "connect"))

    def test_dialect_create_connect_args(self):
        """Test create_connect_args method"""
        from sqlalchemy import URL

        url = URL.create(
            "postgresql+psqlpy",
            username="testuser",
            password="testpass",
            host="localhost",
            port=5432,
            database="testdb",
        )

        args, kwargs = self.dialect.create_connect_args(url)

        self.assertEqual(args, [])  # Returns empty list, not tuple
        self.assertIn("username", kwargs)
        self.assertIn("password", kwargs)
        self.assertIn("host", kwargs)
        self.assertIn("port", kwargs)
        self.assertIn("db_name", kwargs)

    def test_dialect_get_isolation_level(self):
        """Test get_isolation_level method"""
        from unittest.mock import Mock

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = ["read committed"]

        result = self.dialect.get_isolation_level(mock_connection)

        # Should return the isolation level (uppercase)
        self.assertEqual(result, "READ COMMITTED")
        mock_cursor.execute.assert_called_once_with(
            "show transaction isolation level"
        )
        mock_cursor.fetchone.assert_called_once()

    def test_isolation_lookup_property(self):
        """Test _isolation_lookup property"""
        import psqlpy

        isolation_lookup = self.dialect._isolation_lookup

        # Test that the property returns the expected mapping
        self.assertIn("READ_COMMITTED", isolation_lookup)
        self.assertIn("REPEATABLE_READ", isolation_lookup)
        self.assertIn("SERIALIZABLE", isolation_lookup)

        # Test that values are psqlpy isolation levels
        self.assertEqual(
            isolation_lookup["READ_COMMITTED"],
            psqlpy.IsolationLevel.ReadCommitted,
        )
        self.assertEqual(
            isolation_lookup["REPEATABLE_READ"],
            psqlpy.IsolationLevel.RepeatableRead,
        )
        self.assertEqual(
            isolation_lookup["SERIALIZABLE"],
            psqlpy.IsolationLevel.Serializable,
        )

    def test_set_isolation_level(self):
        """Test set_isolation_level method"""
        from unittest.mock import Mock

        mock_connection = Mock()

        # Test setting isolation level
        self.dialect.set_isolation_level(mock_connection, "READ_COMMITTED")

        # Should call set_isolation_level on the connection
        mock_connection.set_isolation_level.assert_called_once()

    def test_set_readonly_true(self):
        """Test set_readonly method with True"""
        from unittest.mock import Mock

        import psqlpy

        mock_connection = Mock()

        self.dialect.set_readonly(mock_connection, True)

        # Should set readonly to ReadOnly
        self.assertEqual(mock_connection.readonly, psqlpy.ReadVariant.ReadOnly)

    def test_set_readonly_false(self):
        """Test set_readonly method with False"""
        from unittest.mock import Mock

        import psqlpy

        mock_connection = Mock()

        self.dialect.set_readonly(mock_connection, False)

        # Should set readonly to ReadWrite
        self.assertEqual(
            mock_connection.readonly, psqlpy.ReadVariant.ReadWrite
        )

    def test_get_readonly(self):
        """Test get_readonly method"""
        from unittest.mock import Mock

        mock_connection = Mock()
        mock_connection.readonly = "test_readonly_value"

        result = self.dialect.get_readonly(mock_connection)

        # Should return the readonly value from connection
        self.assertEqual(result, "test_readonly_value")

    def test_set_deferrable(self):
        """Test set_deferrable method"""
        from unittest.mock import Mock

        mock_connection = Mock()

        self.dialect.set_deferrable(mock_connection, True)

        # Should set deferrable on the connection
        self.assertEqual(mock_connection.deferrable, True)

    def test_get_deferrable(self):
        """Test get_deferrable method"""
        from unittest.mock import Mock

        mock_connection = Mock()
        mock_connection.deferrable = "test_deferrable_value"

        result = self.dialect.get_deferrable(mock_connection)

        # Should return the deferrable value from connection
        self.assertEqual(result, "test_deferrable_value")


if __name__ == "__main__":
    unittest.main()
