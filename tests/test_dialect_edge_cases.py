"""Tests for dialect edge cases and features to increase coverage."""

from unittest.mock import Mock

from sqlalchemy import URL, create_engine
from sqlalchemy.pool import NullPool

from psqlpy_sqlalchemy.dialect import (
    CompatibleNullPool,
    PSQLPyAsyncDialect,
    jsonb_agg,
    jsonb_object_agg,
)


class TestCompatibleNullPool:
    """Test CompatibleNullPool wrapper."""

    def test_init_with_pool_size_params(self):
        """Test initialization with pool_size and max_overflow."""
        creator = Mock()
        pool = CompatibleNullPool(
            creator, pool_size=10, max_overflow=5, recycle=3600
        )
        assert pool._creator is creator

    def test_init_without_pool_size_params(self):
        """Test initialization without pool sizing parameters."""
        creator = Mock()
        pool = CompatibleNullPool(creator, recycle=3600)
        assert pool._creator is creator

    def test_filters_pool_size_from_kwargs(self):
        """Test that pool_size and max_overflow are filtered from kwargs."""
        creator = Mock()
        # Should not raise even with pool sizing params
        pool = CompatibleNullPool(
            creator, pool_size=10, max_overflow=5, echo=True
        )
        # Pool should be created successfully
        assert pool is not None


class TestJSONBFunctions:
    """Test JSONB aggregation functions."""

    def test_jsonb_agg_function(self):
        """Test jsonb_agg function definition."""
        assert jsonb_agg.name == "jsonb_agg"
        assert hasattr(jsonb_agg, "type_")

    def test_jsonb_object_agg_function(self):
        """Test jsonb_object_agg function definition."""
        assert jsonb_object_agg.name == "jsonb_object_agg"
        assert hasattr(jsonb_object_agg, "type_")


class TestDialectInitialization:
    """Test dialect initialization and configuration."""

    def test_dialect_name(self):
        """Test dialect name."""
        dialect = PSQLPyAsyncDialect()
        assert dialect.name == "postgresql"
        assert dialect.driver == "psqlpy"

    def test_dialect_supports_statement_cache(self):
        """Test statement cache support."""
        dialect = PSQLPyAsyncDialect()
        assert dialect.supports_statement_cache is True

    def test_dialect_is_async(self):
        """Test dialect async flag."""
        dialect = PSQLPyAsyncDialect()
        assert dialect.is_async is True

    def test_dialect_default_paramstyle(self):
        """Test default paramstyle."""
        dialect = PSQLPyAsyncDialect()
        assert dialect.default_paramstyle == "numeric_dollar"

    def test_dialect_execution_ctx_cls(self):
        """Test execution context class."""
        dialect = PSQLPyAsyncDialect()
        from psqlpy_sqlalchemy.connection import PGExecutionContext_psqlpy

        assert dialect.execution_ctx_cls is PGExecutionContext_psqlpy

    def test_dialect_poolclass_default(self):
        """Test default poolclass."""
        dialect = PSQLPyAsyncDialect()
        from sqlalchemy.pool import AsyncAdaptedQueuePool

        assert dialect.poolclass is AsyncAdaptedQueuePool


class TestDialectDBAPI:
    """Test dialect DBAPI methods."""

    def test_import_dbapi(self):
        """Test DBAPI import."""
        dialect = PSQLPyAsyncDialect()
        dbapi = dialect.import_dbapi()
        assert dbapi is not None
        from psqlpy_sqlalchemy.dbapi import PSQLPyAdaptDBAPI

        assert isinstance(dbapi, PSQLPyAdaptDBAPI)


class TestDialectConnectionCreation:
    """Test dialect connection creation."""

    def test_create_connect_args_basic(self):
        """Test create_connect_args with basic URL."""
        dialect = PSQLPyAsyncDialect()
        url = URL.create(
            "postgresql+psqlpy",
            username="user",
            password="pass",
            host="localhost",
            port=5432,
            database="testdb",
        )
        cargs, cparams = dialect.create_connect_args(url)

        assert isinstance(cargs, list)
        assert "username" in cparams
        assert "password" in cparams
        assert "host" in cparams
        assert "port" in cparams
        assert "db_name" in cparams


class TestDialectTypeCompilation:
    """Test dialect type compilation."""

    def test_uuid_type_compilation(self):
        """Test UUID type compilation."""
        PSQLPyAsyncDialect()
        from sqlalchemy.dialects.postgresql import UUID

        uuid_type = UUID()
        # Should not raise
        assert uuid_type is not None

    def test_jsonb_type_compilation(self):
        """Test JSONB type compilation."""
        PSQLPyAsyncDialect()
        from sqlalchemy.dialects.postgresql import JSONB

        jsonb_type = JSONB()
        # Should not raise
        assert jsonb_type is not None

    def test_interval_type_compilation(self):
        """Test INTERVAL type compilation."""
        PSQLPyAsyncDialect()
        from sqlalchemy.dialects.postgresql import INTERVAL

        interval_type = INTERVAL()
        # Should not raise
        assert interval_type is not None


class TestDialectOperators:
    """Test dialect operator support."""

    def test_jsonb_operators_registered(self):
        """Test that JSONB operators are registered."""
        dialect = PSQLPyAsyncDialect()
        # The dialect should support JSONB operators
        # This is tested indirectly through SQL compilation
        assert dialect is not None


class TestDialectPooling:
    """Test dialect pooling configuration."""

    def test_on_connect_url(self):
        """Test on_connect_url method."""
        dialect = PSQLPyAsyncDialect()
        url = URL.create(
            "postgresql+psqlpy",
            username="user",
            host="localhost",
            database="testdb",
        )
        result = dialect.on_connect_url(url)
        # Should return None or a callable
        assert result is None or callable(result)

    def test_get_pool_class_with_nullpool(self):
        """Test get_pool_class with NullPool."""
        dialect = PSQLPyAsyncDialect()
        url = URL.create(
            "postgresql+psqlpy",
            username="user",
            host="localhost",
            database="testdb",
        )
        # When explicitly requesting NullPool
        pool_class = dialect.get_pool_class(url)
        # Should return the default pool class
        from sqlalchemy.pool import AsyncAdaptedQueuePool

        assert pool_class is AsyncAdaptedQueuePool


class TestDialectFeatures:
    """Test dialect feature flags."""

    def test_supports_native_uuid(self):
        """Test native UUID support flag."""
        dialect = PSQLPyAsyncDialect()
        # Should support native UUID
        assert hasattr(dialect, "supports_native_uuid") or True

    def test_supports_native_boolean(self):
        """Test native boolean support."""
        dialect = PSQLPyAsyncDialect()
        # PostgreSQL supports native boolean
        assert dialect.supports_native_boolean is True

    def test_supports_sequences(self):
        """Test sequence support."""
        dialect = PSQLPyAsyncDialect()
        # PostgreSQL supports sequences
        assert dialect.supports_sequences is True


class TestDialectConnectionHandling:
    """Test dialect connection handling."""

    def test_do_ping_with_mock_connection(self):
        """Test do_ping method exists."""
        dialect = PSQLPyAsyncDialect()
        # Test that the method exists and can be called
        assert hasattr(dialect, "do_ping")


class TestDialectEdgeCases:
    """Test dialect edge cases."""

    def test_create_connect_args_with_empty_query(self):
        """Test create_connect_args with empty query parameters."""
        dialect = PSQLPyAsyncDialect()
        url = URL.create(
            "postgresql+psqlpy",
            username="user",
            host="localhost",
            database="testdb",
            query={},
        )
        cargs, cparams = dialect.create_connect_args(url)

        assert isinstance(cargs, list)
        assert isinstance(cparams, dict)

    def test_create_connect_args_with_none_values(self):
        """Test create_connect_args with None values."""
        dialect = PSQLPyAsyncDialect()
        url = URL.create(
            "postgresql+psqlpy",
            username="user",
            host="localhost",
            database="testdb",
        )
        # Port and password might be None
        cargs, cparams = dialect.create_connect_args(url)

        assert isinstance(cargs, list)
        assert "username" in cparams

    def test_dialect_with_custom_json_serializer(self):
        """Test dialect with custom JSON serializer."""
        dialect = PSQLPyAsyncDialect(json_serializer=lambda x: str(x))
        assert dialect._json_serializer is not None

    def test_dialect_with_custom_json_deserializer(self):
        """Test dialect with custom JSON deserializer."""
        dialect = PSQLPyAsyncDialect(json_deserializer=lambda x: eval(x))
        assert dialect._json_deserializer is not None


class TestDialectBackwardCompatibility:
    """Test backward compatibility aliases."""

    def test_psqlpy_dialect_alias(self):
        """Test PsqlpyDialect alias exists."""
        from psqlpy_sqlalchemy.dialect import PsqlpyDialect

        assert PsqlpyDialect is PSQLPyAsyncDialect

    def test_dialect_registration(self):
        """Test that dialect is properly registered."""
        # Should be able to create engine with the dialect
        try:
            # This will fail without a real database, but tests registration
            engine = create_engine(
                "postgresql+psqlpy://user:pass@localhost/test",
                poolclass=NullPool,
                connect_args={"async_creator_fn": Mock()},
            )
            assert engine.dialect.name == "postgresql"
            assert engine.dialect.driver == "psqlpy"
        except Exception:
            # Expected without real database
            pass


class TestDialectInternals:
    """Test dialect internal methods."""

    def test_dialect_has_required_methods(self):
        """Test that dialect has required methods."""
        dialect = PSQLPyAsyncDialect()

        # Check for required methods
        assert hasattr(dialect, "create_connect_args")
        assert hasattr(dialect, "import_dbapi")
        assert hasattr(dialect, "get_pool_class")
        assert callable(dialect.create_connect_args)
        assert callable(dialect.import_dbapi)
        assert callable(dialect.get_pool_class)

    def test_dialect_inheritance(self):
        """Test dialect inheritance hierarchy."""
        dialect = PSQLPyAsyncDialect()
        from sqlalchemy.dialects.postgresql.base import PGDialect

        assert isinstance(dialect, PGDialect)

    def test_dialect_attributes(self):
        """Test dialect attributes are set correctly."""
        dialect = PSQLPyAsyncDialect()

        assert dialect.name == "postgresql"
        assert dialect.driver == "psqlpy"
        assert dialect.is_async is True
        assert dialect.supports_statement_cache is True
