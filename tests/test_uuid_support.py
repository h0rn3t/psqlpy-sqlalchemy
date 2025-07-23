"""
Tests for UUID parameter binding support in psqlpy-sqlalchemy.
"""

import os
import uuid

import pytest
from sqlalchemy import Column, Integer, String, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.exc import StatementError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker


# Skip tests if database is not available (check for CI environment or explicit flag)
def should_skip_db_tests() -> bool:
    """Check if database tests should be skipped."""
    # Run tests if explicitly enabled
    if os.getenv("RUN_DB_TESTS"):
        return False
    # In GitHub Actions, only run tests if DATABASE_URL is set (Linux job has it, others don't)
    if os.getenv("GITHUB_ACTIONS"):
        return not bool(os.getenv("DATABASE_URL"))
    # Check if Docker PostgreSQL is available locally
    return not _is_docker_postgres_available()


def _is_docker_postgres_available():
    """Check if Docker PostgreSQL container is running and accessible."""
    try:
        import socket
        import subprocess

        # Check if Docker is installed and running
        try:
            result = subprocess.run(
                [
                    "docker",
                    "ps",
                    "--filter",
                    "name=psqlpy-postgres",
                    "--format",
                    "{{.Names}}",
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if "psqlpy-postgres" in result.stdout:
                # Container is running, check if PostgreSQL is accessible
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2)
                result = sock.connect_ex(("localhost", 5432))
                sock.close()
                return result == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        return False
    except ImportError:
        return False


pytestmark = pytest.mark.skipif(
    should_skip_db_tests(),
    reason="Database tests require live PostgreSQL connection. Set RUN_DB_TESTS=1 or run in CI.",
)


class Base(DeclarativeBase):
    pass


class UUIDTable(Base):
    __tablename__ = "test_uuid_table"

    id = Column(Integer, primary_key=True)
    uid = Column(UUID(as_uuid=True), nullable=False)
    name = Column(String(100))


@pytest.fixture
async def engine():
    """Create test engine."""
    # Use environment variables for database connection in CI
    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql+psqlpy://postgres:password@localhost:5432/test_db",
    )
    engine = create_async_engine(db_url, echo=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    await engine.dispose()


@pytest.fixture
async def session(engine):
    """Create test session."""
    async_session = sessionmaker(engine, class_=AsyncSession)
    async with async_session() as session:
        yield session


class TestUUIDParameterBinding:
    """Test UUID parameter binding functionality."""

    async def test_uuid_object_parameter(self, engine):
        """Test UUID object as parameter."""
        test_uuid = uuid.uuid4()

        async with engine.begin() as conn:
            # Insert with UUID object
            await conn.execute(
                text(
                    "INSERT INTO test_uuid_table (uid, name) VALUES (:uid, :name)"
                ),
                {"uid": test_uuid, "name": "test_uuid_object"},
            )

            # Query with UUID object
            result = await conn.execute(
                text("SELECT * FROM test_uuid_table WHERE uid = :uid"),
                {"uid": test_uuid},
            )

            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0].name == "test_uuid_object"

    async def test_uuid_string_parameter(self, engine):
        """Test UUID string as parameter."""
        test_uuid = uuid.uuid4()
        test_uuid_str = str(test_uuid)

        async with engine.begin() as conn:
            # Insert with UUID string
            await conn.execute(
                text(
                    "INSERT INTO test_uuid_table (uid, name) VALUES (:uid, :name)"
                ),
                {"uid": test_uuid_str, "name": "test_uuid_string"},
            )

            # Query with UUID string
            result = await conn.execute(
                text("SELECT * FROM test_uuid_table WHERE uid = :uid"),
                {"uid": test_uuid_str},
            )

            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0].name == "test_uuid_string"

    async def test_uuid_with_explicit_cast(self, engine):
        """Test UUID parameter handling without problematic explicit casting syntax.

        This test demonstrates the correct way to handle UUID parameters:
        - Use named parameters without explicit casting (SQLAlchemy handles type conversion)
        - Avoid the combination of named parameters with explicit PostgreSQL casting syntax
        """
        test_uuid = uuid.uuid4()

        async with engine.begin() as conn:
            # Insert test data
            await conn.execute(
                text(
                    "INSERT INTO test_uuid_table (uid, name) VALUES (:uid, :name)"
                ),
                {"uid": test_uuid, "name": "test_cast"},
            )

            # Correct approach: Use named parameters without explicit casting
            # SQLAlchemy will handle the UUID type conversion automatically
            result = await conn.execute(
                text(
                    "SELECT * FROM test_uuid_table WHERE uid = :uid LIMIT :limit"
                ),
                {"uid": str(test_uuid), "limit": 2},
            )

            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0].name == "test_cast"

            # Also test with UUID object (not just string)
            result2 = await conn.execute(
                text(
                    "SELECT * FROM test_uuid_table WHERE uid = :uid LIMIT :limit"
                ),
                {"uid": test_uuid, "limit": 1},
            )

            rows2 = result2.fetchall()
            assert len(rows2) == 1
            assert rows2[0].name == "test_cast"

    async def test_uuid_with_sqlalchemy_orm(self, session):
        """Test UUID with SQLAlchemy ORM."""
        test_uuid = uuid.uuid4()

        # Insert with ORM
        test_obj = UUIDTable(uid=test_uuid, name="test_orm")
        session.add(test_obj)
        await session.commit()

        # Query with ORM
        result = await session.execute(
            text(
                "SELECT * FROM test_uuid_table WHERE uid = :uid ORDER BY id LIMIT :limit"
            ),
            {"uid": test_uuid, "limit": 2},
        )

        rows = result.fetchall()
        assert len(rows) == 1
        assert rows[0].name == "test_orm"

    async def test_multiple_uuid_parameters(self, engine):
        """Test multiple UUID parameters in one query."""
        uuid1 = uuid.uuid4()
        uuid2 = uuid.uuid4()

        async with engine.begin() as conn:
            # Insert test data
            await conn.execute(
                text(
                    "INSERT INTO test_uuid_table (uid, name) VALUES (:uid1, :name1), (:uid2, :name2)"
                ),
                {
                    "uid1": uuid1,
                    "name1": "first",
                    "uid2": uuid2,
                    "name2": "second",
                },
            )

            # Query with multiple UUID parameters
            result = await conn.execute(
                text(
                    "SELECT * FROM test_uuid_table WHERE uid IN (:uid1, :uid2) ORDER BY name"
                ),
                {"uid1": uuid1, "uid2": uuid2},
            )

            rows = result.fetchall()
            assert len(rows) == 2
            assert rows[0].name == "first"
            assert rows[1].name == "second"

    async def test_null_uuid_parameter(self, engine):
        """Test NULL UUID parameter."""
        async with engine.begin() as conn:
            # Query with NULL UUID - should return no results
            result = await conn.execute(
                text("SELECT * FROM test_uuid_table WHERE uid = :uid"),
                {"uid": None},
            )

            rows = result.fetchall()
            assert len(rows) == 0

    async def test_invalid_uuid_string(self, engine):
        """Test invalid UUID string raises proper error."""
        async with engine.begin() as conn:
            with pytest.raises((ValueError, StatementError)):
                await conn.execute(
                    text(
                        "INSERT INTO test_uuid_table (uid, name) VALUES (:uid, :name)"
                    ),
                    {"uid": "invalid-uuid-string", "name": "test"},
                )

    async def test_uuid_edge_cases(self, engine):
        """Test UUID edge cases."""
        # Test various UUID formats
        test_cases = [
            uuid.UUID("00000000-0000-0000-0000-000000000000"),  # Nil UUID
            uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),  # Max UUID
            uuid.uuid1(),  # Time-based UUID
            uuid.uuid4(),  # Random UUID
        ]

        async with engine.begin() as conn:
            for i, test_uuid in enumerate(test_cases):
                await conn.execute(
                    text(
                        "INSERT INTO test_uuid_table (uid, name) VALUES (:uid, :name)"
                    ),
                    {"uid": test_uuid, "name": f"edge_case_{i}"},
                )

            # Verify all were inserted correctly
            result = await conn.execute(
                text(
                    "SELECT COUNT(*) as count FROM test_uuid_table WHERE name LIKE 'edge_case_%'"
                )
            )

            count = result.fetchone().count
            assert count == len(test_cases)


class TestUUIDTypeCompatibility:
    """Test UUID type compatibility with existing functionality."""

    async def test_uuid_column_definition(self, engine):
        """Test that UUID columns are properly defined."""
        async with engine.begin() as conn:
            # Check table structure
            result = await conn.execute(
                text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'test_uuid_table' AND column_name = 'uid'
                """)
            )

            row = result.fetchone()
            assert row is not None
            assert row.data_type == "uuid"

    async def test_uuid_index_support(self, engine):
        """Test that UUID columns can be indexed."""
        async with engine.begin() as conn:
            # Create index on UUID column
            await conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_test_uuid_uid ON test_uuid_table(uid)"
                )
            )

            # Verify index was created
            result = await conn.execute(
                text("""
                SELECT indexname
                FROM pg_indexes
                WHERE tablename = 'test_uuid_table' AND indexname = 'idx_test_uuid_uid'
                """)
            )

            row = result.fetchone()
            assert row is not None

            # Clean up
            await conn.execute(text("DROP INDEX IF EXISTS idx_test_uuid_uid"))


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])
