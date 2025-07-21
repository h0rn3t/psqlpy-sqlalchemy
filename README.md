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
- **Type Support**: Native support for PostgreSQL data types

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

## Architecture

The dialect consists of several key components:

- **`PsqlpyDialect`**: Main dialect class that inherits from SQLAlchemy's `DefaultDialect`
- **`PsqlpyDBAPI`**: DBAPI 2.0 compatible interface wrapper
- **`PsqlpyConnection`**: Connection wrapper that adapts psqlpy connections to DBAPI interface
- **`PsqlpyCursor`**: Cursor implementation for executing queries and fetching results

## Limitations

- **Synchronous Only**: Current implementation provides synchronous interface only
- **Basic Transaction Support**: Advanced transaction features may need additional implementation
- **Limited Error Mapping**: psqlpy exceptions are currently mapped to generic DBAPI exceptions

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

### 0.1.0 (2025-07-21)

- Initial release
- Basic SQLAlchemy dialect implementation
- DBAPI 2.0 compatible interface
- SQLModel compatibility
- Connection string parsing
- Basic SQL compilation support
- Transaction support
- SSL configuration support
