#!/usr/bin/env python3
"""
Performance comparison between psqlpy-sqlalchemy and asyncpg dialect.

This script benchmarks various database operations to measure performance improvements.
"""

import asyncio
import time
import typing as t
from statistics import mean, median, stdev
from typing import Dict, List

from sqlalchemy import (
    Integer,
    String,
    Text,
    text,
)
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Database connection strings
PSQLPY_URL = "postgresql+psqlpy://postgres:password@localhost:5432/test_db"
ASYNCPG_URL = "postgresql+asyncpg://postgres:password@localhost:5432/test_db"


class Base(DeclarativeBase):
    """Declarative base class for ORM models."""


class TestModel(Base):
    """Test model for benchmarking."""

    __tablename__ = "benchmark_test"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[t.Optional[str]] = mapped_column(Text)
    value: Mapped[int] = mapped_column(Integer, default=0)


class BenchmarkResult:
    """Store and format benchmark results."""

    def __init__(self, name: str):
        self.name = name
        self.times: List[float] = []

    def add_time(self, duration: float) -> None:
        """Add a timing measurement."""
        self.times.append(duration)

    def get_stats(self) -> Dict[str, float]:
        """Calculate statistics for the benchmark."""
        if not self.times:
            return {"mean": 0, "median": 0, "stdev": 0, "min": 0, "max": 0}

        return {
            "mean": mean(self.times),
            "median": median(self.times),
            "stdev": stdev(self.times) if len(self.times) > 1 else 0,
            "min": min(self.times),
            "max": max(self.times),
        }

    def __str__(self) -> str:
        """Format results as string."""
        stats = self.get_stats()
        return (
            f"{self.name}:\n"
            f"  Mean:   {stats['mean'] * 1000:.2f}ms\n"
            f"  Median: {stats['median'] * 1000:.2f}ms\n"
            f"  StdDev: {stats['stdev'] * 1000:.2f}ms\n"
            f"  Min:    {stats['min'] * 1000:.2f}ms\n"
            f"  Max:    {stats['max'] * 1000:.2f}ms"
        )


async def setup_database(url: str) -> None:
    """Set up the test database."""
    engine = create_async_engine(url, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()


async def benchmark_simple_select(
    url: str, iterations: int = 100
) -> BenchmarkResult:
    """Benchmark simple SELECT queries."""
    result = BenchmarkResult("Simple SELECT")
    engine = create_async_engine(
        url, echo=False, pool_size=10, max_overflow=20
    )

    try:
        async with engine.begin() as conn:
            # Insert test data
            for i in range(100):
                await conn.execute(
                    text(
                        "INSERT INTO benchmark_test (name, description, value) VALUES (:name, :desc, :val)"
                    ),
                    {
                        "name": f"test_{i}",
                        "desc": f"Description {i}",
                        "val": i,
                    },
                )

        for _ in range(iterations):
            start = time.perf_counter()
            async with engine.connect() as conn:
                result_proxy = await conn.execute(
                    text("SELECT * FROM benchmark_test LIMIT 10")
                )
                result_proxy.fetchall()
            end = time.perf_counter()
            result.add_time(end - start)

    finally:
        await engine.dispose()

    return result


async def benchmark_bulk_insert(
    url: str, batch_size: int = 1000, iterations: int = 10
) -> BenchmarkResult:
    """Benchmark bulk INSERT operations."""
    result = BenchmarkResult(f"Bulk INSERT ({batch_size} rows)")
    engine = create_async_engine(
        url, echo=False, pool_size=10, max_overflow=20
    )

    try:
        for _iteration in range(iterations):
            # Clear table before each iteration
            async with engine.begin() as conn:
                await conn.execute(text("TRUNCATE benchmark_test"))

            start = time.perf_counter()
            async with engine.begin() as conn:
                for i in range(batch_size):
                    await conn.execute(
                        text(
                            "INSERT INTO benchmark_test (name, description, value) VALUES (:name, :desc, :val)"
                        ),
                        {
                            "name": f"test_{i}",
                            "desc": f"Description {i}",
                            "val": i,
                        },
                    )
            end = time.perf_counter()
            result.add_time(end - start)

    finally:
        await engine.dispose()

    return result


async def benchmark_executemany(
    url: str, batch_size: int = 1000, iterations: int = 10
) -> BenchmarkResult:
    """Benchmark executemany operations."""
    result = BenchmarkResult(f"executemany ({batch_size} rows)")
    engine = create_async_engine(
        url, echo=False, pool_size=10, max_overflow=20
    )

    try:
        for _iteration in range(iterations):
            # Clear table before each iteration
            async with engine.begin() as conn:
                await conn.execute(text("TRUNCATE benchmark_test"))

            # Prepare batch data
            batch_data = [
                {"name": f"test_{i}", "desc": f"Description {i}", "val": i}
                for i in range(batch_size)
            ]

            start = time.perf_counter()
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO benchmark_test (name, description, value) VALUES (:name, :desc, :val)"
                    ),
                    batch_data,
                )
            end = time.perf_counter()
            result.add_time(end - start)

    finally:
        await engine.dispose()

    return result


async def benchmark_complex_query(
    url: str, iterations: int = 100
) -> BenchmarkResult:
    """Benchmark complex queries with aggregations."""
    result = BenchmarkResult("Complex query with aggregation")
    engine = create_async_engine(
        url, echo=False, pool_size=10, max_overflow=20
    )

    try:
        # Populate with test data
        async with engine.begin() as conn:
            await conn.execute(text("TRUNCATE benchmark_test"))
            for i in range(1000):
                await conn.execute(
                    text(
                        "INSERT INTO benchmark_test (name, description, value) VALUES (:name, :desc, :val)"
                    ),
                    {
                        "name": f"test_{i % 10}",
                        "desc": f"Description {i}",
                        "val": i,
                    },
                )

        complex_query = text("""
            SELECT
                name,
                COUNT(*) as count,
                AVG(value) as avg_value,
                MAX(value) as max_value,
                MIN(value) as min_value
            FROM benchmark_test
            GROUP BY name
            HAVING COUNT(*) > 10
            ORDER BY count DESC
        """)

        for _ in range(iterations):
            start = time.perf_counter()
            async with engine.connect() as conn:
                result_proxy = await conn.execute(complex_query)
                result_proxy.fetchall()
            end = time.perf_counter()
            result.add_time(end - start)

    finally:
        await engine.dispose()

    return result


async def benchmark_transaction(
    url: str, iterations: int = 50
) -> BenchmarkResult:
    """Benchmark transaction performance."""
    result = BenchmarkResult("Transaction with multiple operations")
    engine = create_async_engine(
        url, echo=False, pool_size=10, max_overflow=20
    )

    try:
        for _ in range(iterations):
            start = time.perf_counter()
            async with engine.begin() as conn:
                # Multiple operations within transaction
                await conn.execute(
                    text(
                        "INSERT INTO benchmark_test (name, description, value) VALUES (:name, :desc, :val)"
                    ),
                    {"name": "tx_test", "desc": "Transaction test", "val": 1},
                )
                await conn.execute(
                    text(
                        "UPDATE benchmark_test SET value = value + 1 WHERE name = :name"
                    ),
                    {"name": "tx_test"},
                )
                await conn.execute(
                    text("SELECT * FROM benchmark_test WHERE name = :name"),
                    {"name": "tx_test"},
                )
                await conn.execute(
                    text("DELETE FROM benchmark_test WHERE name = :name"),
                    {"name": "tx_test"},
                )
            end = time.perf_counter()
            result.add_time(end - start)

    finally:
        await engine.dispose()

    return result


async def run_benchmarks(
    url: str, dialect_name: str
) -> Dict[str, BenchmarkResult]:
    """Run all benchmarks for a specific dialect."""
    print(f"\n{'=' * 60}")
    print(f"Running benchmarks for {dialect_name}")
    print(f"{'=' * 60}\n")

    results = {}

    # Setup database
    print("Setting up database...")
    await setup_database(url)

    # Run benchmarks
    print("Running simple SELECT benchmark...")
    results["simple_select"] = await benchmark_simple_select(
        url, iterations=100
    )

    print("Running bulk INSERT benchmark...")
    results["bulk_insert"] = await benchmark_bulk_insert(
        url, batch_size=1000, iterations=10
    )

    print("Running executemany benchmark...")
    results["executemany"] = await benchmark_executemany(
        url, batch_size=1000, iterations=10
    )

    print("Running complex query benchmark...")
    results["complex_query"] = await benchmark_complex_query(
        url, iterations=100
    )

    print("Running transaction benchmark...")
    results["transaction"] = await benchmark_transaction(url, iterations=50)

    return results


def print_comparison(
    psqlpy_results: Dict[str, BenchmarkResult],
    asyncpg_results: Dict[str, BenchmarkResult],
) -> None:
    """Print comparison of results."""
    print(f"\n{'=' * 60}")
    print("PERFORMANCE COMPARISON RESULTS")
    print(f"{'=' * 60}\n")

    for benchmark_name in psqlpy_results:
        psqlpy_stats = psqlpy_results[benchmark_name].get_stats()
        asyncpg_stats = asyncpg_results[benchmark_name].get_stats()

        psqlpy_mean = psqlpy_stats["mean"] * 1000  # Convert to ms
        asyncpg_mean = asyncpg_stats["mean"] * 1000  # Convert to ms

        improvement = (
            ((asyncpg_mean - psqlpy_mean) / asyncpg_mean) * 100
            if asyncpg_mean > 0
            else 0
        )

        print(f"\n{benchmark_name.upper().replace('_', ' ')}:")
        print(f"  psqlpy-sqlalchemy: {psqlpy_mean:.2f}ms (mean)")
        print(f"  asyncpg:           {asyncpg_mean:.2f}ms (mean)")

        if improvement > 0:
            print(f"  ✓ psqlpy is {improvement:.1f}% FASTER")
        elif improvement < 0:
            print(f"  ✗ psqlpy is {abs(improvement):.1f}% SLOWER")
        else:
            print("  = Performance is EQUAL")

    print(f"\n{'=' * 60}")


async def main() -> int:
    """Main benchmark runner."""
    print("PostgreSQL SQLAlchemy Dialect Performance Comparison")
    print("Comparing psqlpy-sqlalchemy vs asyncpg")
    print("\nThis may take several minutes...\n")

    try:
        # Run psqlpy benchmarks
        psqlpy_results = await run_benchmarks(PSQLPY_URL, "psqlpy-sqlalchemy")

        # Run asyncpg benchmarks
        asyncpg_results = await run_benchmarks(ASYNCPG_URL, "asyncpg")

        # Print detailed results
        print("\n" + "=" * 60)
        print("DETAILED RESULTS")
        print("=" * 60)

        print("\n--- psqlpy-sqlalchemy ---")
        for result in psqlpy_results.values():
            print(f"\n{result}")

        print("\n--- asyncpg ---")
        for result in asyncpg_results.values():
            print(f"\n{result}")

        # Print comparison
        print_comparison(psqlpy_results, asyncpg_results)

    except Exception as e:
        print(f"\n❌ Error running benchmarks: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
