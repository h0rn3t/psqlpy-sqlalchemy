"""Tests for DBAPI edge cases and type constructors to increase coverage."""

import datetime

from psqlpy_sqlalchemy.dbapi import PsqlpyDBAPI


class TestPsqlpyDBAPI:
    """Test PsqlpyDBAPI type constructors and methods."""

    def test_date_constructor(self):
        """Test Date type constructor."""
        dbapi = PsqlpyDBAPI()
        result = dbapi.Date(2023, 12, 25)
        assert isinstance(result, datetime.date)
        assert result.year == 2023
        assert result.month == 12
        assert result.day == 25

    def test_time_constructor(self):
        """Test Time type constructor."""
        dbapi = PsqlpyDBAPI()
        result = dbapi.Time(14, 30, 45)
        assert isinstance(result, datetime.time)
        assert result.hour == 14
        assert result.minute == 30
        assert result.second == 45

    def test_timestamp_constructor(self):
        """Test Timestamp type constructor."""
        dbapi = PsqlpyDBAPI()
        result = dbapi.Timestamp(2023, 12, 25, 14, 30, 45)
        assert isinstance(result, datetime.datetime)
        assert result.year == 2023
        assert result.month == 12
        assert result.day == 25
        assert result.hour == 14
        assert result.minute == 30
        assert result.second == 45

    def test_date_from_ticks(self):
        """Test DateFromTicks constructor."""
        dbapi = PsqlpyDBAPI()
        # Use a known timestamp (2023-01-01 00:00:00 UTC)
        ticks = 1672531200.0
        result = dbapi.DateFromTicks(ticks)
        assert isinstance(result, datetime.date)

    def test_time_from_ticks(self):
        """Test TimeFromTicks constructor."""
        dbapi = PsqlpyDBAPI()
        # Use a known timestamp
        ticks = 1672531200.0
        result = dbapi.TimeFromTicks(ticks)
        assert isinstance(result, datetime.time)

    def test_timestamp_from_ticks(self):
        """Test TimestampFromTicks constructor."""
        dbapi = PsqlpyDBAPI()
        # Use a known timestamp
        ticks = 1672531200.0
        result = dbapi.TimestampFromTicks(ticks)
        assert isinstance(result, datetime.datetime)

    def test_binary_constructor_with_string(self):
        """Test Binary constructor with string input."""
        dbapi = PsqlpyDBAPI()
        result = dbapi.Binary("test string")
        assert isinstance(result, bytes)
        assert result == b"test string"

    def test_binary_constructor_with_bytes(self):
        """Test Binary constructor with bytes input."""
        dbapi = PsqlpyDBAPI()
        input_bytes = b"test bytes"
        result = dbapi.Binary(input_bytes)
        assert isinstance(result, bytes)
        assert result == input_bytes

    def test_type_objects(self):
        """Test type objects for type comparison."""
        dbapi = PsqlpyDBAPI()

        assert dbapi.STRING is str
        assert dbapi.BINARY is bytes
        assert (int, float) == dbapi.NUMBER
        assert dbapi.DATETIME is object
        assert dbapi.ROWID is int

    def test_dbapi_attributes(self):
        """Test DBAPI 2.0 attributes."""
        dbapi = PsqlpyDBAPI()

        assert dbapi.apilevel == "2.0"
        assert dbapi.threadsafety == 2
        assert dbapi.paramstyle == "numeric_dollar"

    def test_exception_hierarchy(self):
        """Test exception hierarchy setup."""
        dbapi = PsqlpyDBAPI()

        # All exceptions should be set
        assert dbapi.Warning is not None
        assert dbapi.Error is not None
        assert dbapi.InterfaceError is not None
        assert dbapi.DatabaseError is not None
        assert dbapi.DataError is not None
        assert dbapi.OperationalError is not None
        assert dbapi.IntegrityError is not None
        assert dbapi.InternalError is not None
        assert dbapi.ProgrammingError is not None
        assert dbapi.NotSupportedError is not None


class TestDBAPIEdgeCases:
    """Test DBAPI edge cases."""

    def test_binary_with_empty_string(self):
        """Test Binary constructor with empty string."""
        dbapi = PsqlpyDBAPI()
        result = dbapi.Binary("")
        assert isinstance(result, bytes)
        assert result == b""

    def test_binary_with_empty_bytes(self):
        """Test Binary constructor with empty bytes."""
        dbapi = PsqlpyDBAPI()
        result = dbapi.Binary(b"")
        assert isinstance(result, bytes)
        assert result == b""

    def test_date_edge_cases(self):
        """Test Date constructor with edge cases."""
        dbapi = PsqlpyDBAPI()

        # Test leap year
        result = dbapi.Date(2024, 2, 29)
        assert result.year == 2024
        assert result.month == 2
        assert result.day == 29

    def test_time_edge_cases(self):
        """Test Time constructor with edge cases."""
        dbapi = PsqlpyDBAPI()

        # Test midnight
        result = dbapi.Time(0, 0, 0)
        assert result.hour == 0
        assert result.minute == 0
        assert result.second == 0

        # Test end of day
        result = dbapi.Time(23, 59, 59)
        assert result.hour == 23
        assert result.minute == 59
        assert result.second == 59

    def test_timestamp_edge_cases(self):
        """Test Timestamp constructor with edge cases."""
        dbapi = PsqlpyDBAPI()

        # Test epoch
        result = dbapi.Timestamp(1970, 1, 1, 0, 0, 0)
        assert result.year == 1970
        assert result.month == 1
        assert result.day == 1
        assert result.hour == 0
        assert result.minute == 0
        assert result.second == 0

    def test_ticks_constructors_with_zero(self):
        """Test tick-based constructors with zero."""
        dbapi = PsqlpyDBAPI()

        # Test with zero ticks (epoch)
        date_result = dbapi.DateFromTicks(0)
        assert isinstance(date_result, datetime.date)

        time_result = dbapi.TimeFromTicks(0)
        assert isinstance(time_result, datetime.time)

        timestamp_result = dbapi.TimestampFromTicks(0)
        assert isinstance(timestamp_result, datetime.datetime)
