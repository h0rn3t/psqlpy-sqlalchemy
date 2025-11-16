# psqlpy-sqlalchemy
[![ci](https://img.shields.io/badge/Support-Ukraine-FFD500?style=flat&labelColor=005BBB)](https://img.shields.io/badge/Support-Ukraine-FFD500?style=flat&labelColor=005BBB)
[![ci](https://github.com/h0rn3t/psqlpy-sqlalchemy/workflows/ci/badge.svg)](https://github.com/h0rn3t/psqlpy-sqlalchemy/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/h0rn3t/psqlpy-sqlalchemy/graph/badge.svg?token=tZoyeATPa2)](https://codecov.io/gh/h0rn3t/psqlpy-sqlalchemy)[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![pip](https://img.shields.io/pypi/v/psqlpy-sqlalchemy?color=blue)](https://pypi.org/project/psqlpy-sqlalchemy/)
[![Updates](https://pyup.io/repos/github/h0rn3t/psqlpy-sqlalchemy/shield.svg)](https://pyup.io/repos/github/h0rn3t/psqlpy-sqlalchemy/)

SQLAlchemy dialect for [psqlpy](https://github.com/qaspen-python/psqlpy) - a fast PostgreSQL driver for Python.



## Overview

This package provides a SQLAlchemy dialect that allows you to use psqlpy as the underlying PostgreSQL driver. psqlpy is a high-performance PostgreSQL driver built on top of Rust's tokio-postgres, offering excellent performance characteristics.

## Features

- **High Performance**: Built on psqlpy's Rust-based PostgreSQL driver
- **SQLAlchemy 2.0+ Compatible**: Full support for modern SQLAlchemy features
- **SQLModel Compatible**: Works with SQLModel for Pydantic integration
- **DBAPI 2.0 Compliant**: Standard Python database interface
- **Connection Pooling**: Leverages psqlpy's built-in connection pooling
- **Transaction Support**: Full transaction and savepoint support
- **SSL Support**: Configurable SSL connections
- **Advanced Type Support**:
  - Native UUID support with efficient caching
  - Full JSONB operator support (@>, <@, ?, ?&, ?|, etc.)
  - PostgreSQL array types
  - Custom type conversion with automatic detection

## Installation

```bash
pip install psqlpy-sqlalchemy
```

This will automatically install the required dependencies:
- `sqlalchemy>=2.0.0`
- `psqlpy>=0.11.0`

## Usage

### Basic Connection

```python
from sqlalchemy import create_engine

# Basic connection
engine = create_engine("postgresql+psqlpy://user:password@localhost/dbname")

# With connection parameters
engine = create_engine(
    "postgresql+psqlpy://user:password@localhost:5432/dbname"
    "?sslmode=require&application_name=myapp"
)
```

### Connection URL Parameters

The dialect supports standard PostgreSQL connection parameters:

- `host` - Database host
- `port` - Database port (default: 5432)
- `username` - Database username
- `password` - Database password
- `database` - Database name
- `sslmode` - SSL mode (disable, allow, prefer, require, verify-ca, verify-full)
- `application_name` - Application name for connection tracking
- `connect_timeout` - Connection timeout in seconds

### Example Usage

```python
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String
from sqlalchemy.orm import sessionmaker

# Create engine
engine = create_engine("postgresql+psqlpy://user:password@localhost/testdb")

# Test connection
with engine.connect() as conn:
    result = conn.execute(text("SELECT version()"))
    print(result.fetchone())

# Using ORM
Session = sessionmaker(bind=engine)
session = Session()

# Define a table
metadata = MetaData()
users = Table('users', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(50)),
    Column('email', String(100))
)

# Create table
metadata.create_all(engine)

# Insert data
with engine.connect() as conn:
    conn.execute(users.insert().values(name='John', email='john@example.com'))
    conn.commit()

# Query data
with engine.connect() as conn:
    result = conn.execute(users.select())
    for row in result:
        print(row)
```

### UUID Support

The dialect provides native UUID support with automatic conversion:

```python
from sqlalchemy import create_engine, Column, text
from sqlalchemy.dialects.postgresql import UUID
import uuid

engine = create_engine("postgresql+psqlpy://user:password@localhost/testdb")

# Using UUID columns
with engine.connect() as conn:
    # UUID objects are automatically converted
    user_id = uuid.uuid4()
    conn.execute(
        text("INSERT INTO users (id, name) VALUES (:id, :name)"),
        {"id": user_id, "name": "John"}
    )

    # UUID strings are also supported
    conn.execute(
        text("SELECT * FROM users WHERE id = :id"),
        {"id": "550e8400-e29b-41d4-a716-446655440000"}
    )

    # Explicit casting (recommended for clarity)
    conn.execute(
        text("SELECT * FROM users WHERE id = :id::UUID"),
        {"id": user_id}
    )
    conn.commit()
```

### JSONB Support

Full support for PostgreSQL JSONB operators:

```python
from sqlalchemy import create_engine, Column, Integer, text
from sqlalchemy.dialects.postgresql import JSONB

engine = create_engine("postgresql+psqlpy://user:password@localhost/testdb")

with engine.connect() as conn:
    # JSONB contains operator (@>)
    conn.execute(
        text("SELECT * FROM products WHERE metadata @> :filter"),
        {"filter": {"color": "red"}}
    )

    # JSONB path operators
    conn.execute(
        text("SELECT metadata->>'name' FROM products WHERE id = :id"),
        {"id": 1}
    )

    # JSONB existence operators
    conn.execute(
        text("SELECT * FROM products WHERE metadata ? :key"),
        {"key": "color"}
    )
    conn.commit()
```

### Bulk INSERT Operations

The dialect automatically optimizes bulk INSERT operations:

```python
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData

engine = create_engine("postgresql+psqlpy://user:password@localhost/testdb")
metadata = MetaData()

users = Table('users', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(50)),
    Column('age', Integer)
)

# Bulk insert - automatically uses multi-value INSERT
data = [
    {"name": f"User{i}", "age": 20 + i}
    for i in range(1000)
]

with engine.begin() as conn:
    # This is converted to a single multi-value INSERT
    # INSERT INTO users (name, age) VALUES ($1, $2), ($3, $4), ..., ($1999, $2000)
    conn.execute(users.insert(), data)
    # ~23x faster than executing 1000 separate INSERT statements!
```

### SQLModel Usage

```python
from typing import Optional
from sqlmodel import Field, Session, SQLModel, create_engine, select

# Define a SQLModel model
class Hero(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    secret_name: str
    age: Optional[int] = None

# Create engine with psqlpy dialect
engine = create_engine("postgresql+psqlpy://user:password@localhost/testdb")

# Create tables
SQLModel.metadata.create_all(engine)

# Insert data
with Session(engine) as session:
    hero = Hero(name="Deadpond", secret_name="Dive Wilson", age=30)
    session.add(hero)
    session.commit()
    session.refresh(hero)
    print(f"Created hero: {hero.name} with id {hero.id}")

# Query data
with Session(engine) as session:
    statement = select(Hero).where(Hero.name == "Deadpond")
    hero = session.exec(statement).first()
    print(f"Found hero: {hero.name}, secret identity: {hero.secret_name}")
```

### Async Usage

While this dialect provides a synchronous interface, psqlpy itself is async-native. For async SQLAlchemy usage, you would typically use SQLAlchemy's async features:

```python
from sqlalchemy.ext.asyncio import create_async_engine

# Note: This would require an async version of the dialect
# The current implementation is synchronous
engine = create_engine("postgresql+psqlpy://user:password@localhost/dbname")
```

## Configuration

### SSL Configuration

```python
# Require SSL
engine = create_engine("postgresql+psqlpy://user:pass@host/db?sslmode=require")

# SSL with custom CA file
engine = create_engine("postgresql+psqlpy://user:pass@host/db?sslmode=verify-ca&ca_file=/path/to/ca.pem")
```

### Connection Timeouts

```python
# Set connection timeout
engine = create_engine("postgresql+psqlpy://user:pass@host/db?connect_timeout=30")
```

## Development

### Setting up Development Environment

```bash
# Clone the repository
git clone https://github.com/your-username/psqlpy-sqlalchemy.git
cd psqlpy-sqlalchemy

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install in development mode
pip install -e .

# Install development dependencies
pip install -e ".[dev]"
```

### Running Tests

```bash
pytest tests/
```

### Testing with Real Database

To test with a real PostgreSQL database:

```python
from sqlalchemy import create_engine, text

# Replace with your actual database credentials
engine = create_engine("postgresql+psqlpy://user:password@localhost/testdb")

with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))
    print("Connection successful:", result.fetchone())
```

### Performance Benchmarking

The project includes a comprehensive performance comparison test between psqlpy-sqlalchemy and asyncpg:

```bash
# Run performance benchmark (recommended)
make benchmark
```

This command will:
- Automatically start PostgreSQL if not running
- Install required dependencies
- Run performance comparison tests across multiple scenarios
- Clean up resources after completion

The benchmark tests various operations including:
- Simple SELECT queries
- Single and bulk INSERT operations
- Complex queries with aggregations
- Concurrent operations

For detailed benchmark configuration and results interpretation, see [PERFORMANCE_TEST_README.md](PERFORMANCE_TEST_README.md).

## Architecture

The dialect consists of several key components:

- **`PSQLPyAsyncDialect`** (`dialect.py`): Main dialect class inheriting from PostgreSQL base dialect
  - Handles SQL compilation and type mapping
  - Manages connection creation and pooling
  - Provides asyncpg-compatible naming conventions for migration

- **`PSQLPyAdaptDBAPI`** (`dbapi.py`): DBAPI 2.0 compliant interface wrapper
  - Adapts psqlpy to SQLAlchemy's expected interface
  - Provides standard exception hierarchy

- **`AsyncAdapt_psqlpy_connection`** (`connection.py`): Connection adapter
  - Bridges psqlpy's async connections to SQLAlchemy's synchronous interface using `await_only`
  - Implements transaction management with savepoint support
  - Provides connection health checking with `ping()` method

- **`AsyncAdapt_psqlpy_cursor`** (`connection.py`): Cursor implementation
  - Handles query execution with parameter binding
  - Implements multi-value INSERT optimization for bulk operations
  - Supports both regular and server-side cursors
  - Automatic conversion of named parameters to positional ($1, $2, etc.)

**Backward Compatibility**: Aliases `PsqlpyDialect`, `PsqlpyConnection`, and `PsqlpyCursor` are provided for compatibility.

### Protocol-Level Batching

For UPDATE/DELETE operations within transactions, the dialect uses psqlpy's `transaction.pipeline()`:

```python
with engine.begin() as conn:
    # These updates are batched into a single network round-trip
    conn.execute(users.update().where(users.c.id == 1).values(name="John"))
    conn.execute(users.update().where(users.c.id == 2).values(name="Jane"))
```

This reduces network latency by sending multiple commands in a single batch.

### Type Conversion Caching

- UUID conversion results are cached to avoid repeated parsing
- Parameter type detection uses cached regex patterns
- Prepared statement metadata is cached when possible

For detailed performance benchmarks, run `make benchmark` or see [PERFORMANCE_TEST_README.md](PERFORMANCE_TEST_README.md).

## Limitations and Design Considerations

- **Prepared Statement Reuse**: psqlpy's Python API requires parameters at prepare() time, preventing prepared statement caching like asyncpg.
- **Error Mapping**: All psqlpy exceptions are mapped to a single `psqlpy.Error` class for DBAPI compatibility

## Contributing

Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests.

### Development Guidelines

1. Follow PEP 8 style guidelines
2. Add tests for new features
3. Update documentation as needed
4. Ensure compatibility with SQLAlchemy 2.0+

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Related Projects

- [psqlpy](https://github.com/qaspen-python/psqlpy) - The underlying PostgreSQL driver
- [SQLAlchemy](https://www.sqlalchemy.org/) - The Python SQL toolkit and ORM
- [SQLModel](https://sqlmodel.tiangolo.com/) - SQLAlchemy-based ORM with Pydantic validation

## Changelog

### 0.1.0a12 (Current)

**Performance Optimizations:**
- Implemented multi-value INSERT optimization for bulk operations (23.5x speedup for 100 rows)
- Added transaction.pipeline() support for UPDATE/DELETE batching
- Implemented prepared statement caching with automatic type inference
- Added schema cache invalidation tracking

**Type Support Enhancements:**
- Native UUID support with efficient byte conversion and caching
- Full JSONB operator support (@>, <@, ?, ?&, ?|, ->, ->>, #>, #>>, ||, -, #-)
- Improved parameter type conversion with caching

**API Improvements:**
- Added asyncpg-compatible attribute naming for easier migration
- Implemented connection health checking with ping() method
- Added transaction savepoint support
- Improved error messages for UUID casting issues

**Code Quality:**
- Removed performance tracking overhead
- Optimized connection and cursor implementations
- Enhanced documentation with technical details
- Comprehensive test coverage

### 0.1.0a11

- Initial alpha release
- Basic SQLAlchemy dialect implementation
- DBAPI 2.0 compatible interface
- SQLModel compatibility
- Connection string parsing
- Basic SQL compilation support
- Transaction support
- SSL configuration support
