#!/usr/bin/env python3
"""
Unit tests for psqlpy-sqlalchemy DBAPI interface
"""

import datetime
import unittest
from unittest.mock import Mock, patch

from psqlpy_sqlalchemy.dbapi import PSQLPyAdaptDBAPI, PsqlpyDBAPI


class TestPsqlpyDBAPI(unittest.TestCase):
    """Test cases for the psqlpy DBAPI interface"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.dbapi = PsqlpyDBAPI()

    def test_dbapi_attributes(self):
        """Test DBAPI 2.0 module attributes"""
        self.assertEqual(self.dbapi.apilevel, "2.0")
        self.assertEqual(self.dbapi.threadsafety, 2)
        self.assertEqual(self.dbapi.paramstyle, "numeric_dollar")

    def test_exception_hierarchy(self):
        """Test that all required DBAPI exceptions are available"""
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
                hasattr(self.dbapi, exc_name),
                f"Missing DBAPI exception: {exc_name}",
            )
            exc_class = getattr(self.dbapi, exc_name)
            self.assertTrue(
                callable(exc_class), f"Exception {exc_name} is not callable"
            )

    def test_type_constructors(self):
        """Test DBAPI type constructors"""
        # Test Date constructor
        date_obj = self.dbapi.Date(2023, 12, 25)
        self.assertIsInstance(date_obj, datetime.date)
        self.assertEqual(date_obj.year, 2023)
        self.assertEqual(date_obj.month, 12)
        self.assertEqual(date_obj.day, 25)

        # Test Time constructor
        time_obj = self.dbapi.Time(14, 30, 45)
        self.assertIsInstance(time_obj, datetime.time)
        self.assertEqual(time_obj.hour, 14)
        self.assertEqual(time_obj.minute, 30)
        self.assertEqual(time_obj.second, 45)

        # Test Timestamp constructor
        timestamp_obj = self.dbapi.Timestamp(2023, 12, 25, 14, 30, 45)
        self.assertIsInstance(timestamp_obj, datetime.datetime)
        self.assertEqual(timestamp_obj.year, 2023)
        self.assertEqual(timestamp_obj.month, 12)
        self.assertEqual(timestamp_obj.day, 25)
        self.assertEqual(timestamp_obj.hour, 14)
        self.assertEqual(timestamp_obj.minute, 30)
        self.assertEqual(timestamp_obj.second, 45)

    def test_binary_constructor(self):
        """Test Binary type constructor"""
        # Test with string
        binary_obj = self.dbapi.Binary("test string")
        self.assertIsInstance(binary_obj, bytes)
        self.assertEqual(binary_obj, b"test string")

        # Test with bytes
        binary_obj2 = self.dbapi.Binary(b"test bytes")
        self.assertIsInstance(binary_obj2, bytes)
        self.assertEqual(binary_obj2, b"test bytes")

        # Test with list
        binary_obj3 = self.dbapi.Binary([65, 66, 67])
        self.assertIsInstance(binary_obj3, bytes)
        self.assertEqual(binary_obj3, b"ABC")

    def test_tick_constructors(self):
        """Test constructors that work with timestamps"""
        import time

        # Get current timestamp
        current_time = time.time()

        # Test DateFromTicks
        date_from_ticks = self.dbapi.DateFromTicks(current_time)
        self.assertIsInstance(date_from_ticks, datetime.date)

        # Test TimeFromTicks
        time_from_ticks = self.dbapi.TimeFromTicks(current_time)
        self.assertIsInstance(time_from_ticks, datetime.time)

        # Test TimestampFromTicks
        timestamp_from_ticks = self.dbapi.TimestampFromTicks(current_time)
        self.assertIsInstance(timestamp_from_ticks, datetime.datetime)

    def test_type_objects(self):
        """Test type objects for type comparison"""
        self.assertEqual(self.dbapi.STRING, str)
        self.assertEqual(self.dbapi.BINARY, bytes)
        self.assertEqual(self.dbapi.NUMBER, (int, float))
        self.assertEqual(self.dbapi.ROWID, int)
        self.assertIsNotNone(self.dbapi.DATETIME)

    def test_connect_method_exists(self):
        """Test that connect method exists"""
        self.assertTrue(hasattr(self.dbapi, "connect"))
        self.assertTrue(callable(self.dbapi.connect))

    def test_connect_method_delegation(self):
        """Test that PsqlpyDBAPI.connect delegates to adapted DBAPI"""
        with patch.object(self.dbapi._adapt_dbapi, "connect") as mock_connect:
            mock_connect.return_value = Mock()

            result = self.dbapi.connect("test_arg", test_kwarg="test_value")

            mock_connect.assert_called_once_with(
                "test_arg", test_kwarg="test_value"
            )
            self.assertEqual(result, mock_connect.return_value)


class TestPSQLPyAdaptDBAPI(unittest.TestCase):
    """Test cases for PSQLPyAdaptDBAPI class"""

    def setUp(self):
        """Set up test fixtures"""
        # Create a mock psqlpy module
        self.mock_psqlpy = Mock()
        self.mock_psqlpy.Error = Exception
        self.mock_psqlpy.connect = Mock()

        self.adapt_dbapi = PSQLPyAdaptDBAPI(self.mock_psqlpy)

    def test_server_settings_handling(self):
        """Test server_settings parameter handling"""
        with patch(
            "psqlpy_sqlalchemy.dbapi.AsyncAdapt_psqlpy_connection"
        ), patch("psqlpy_sqlalchemy.dbapi.await_only") as mock_await_only:
            mock_connection = Mock()
            mock_await_only.return_value = mock_connection

            # Test with server_settings containing application_name
            server_settings = {
                "application_name": "test_app",
                "other_setting": "value",
            }

            self.adapt_dbapi.connect(
                host="localhost",
                port=5432,
                server_settings=server_settings,
                unsupported_param="should_be_filtered",
            )

            # Verify that application_name was extracted from server_settings
            mock_await_only.call_args[0][
                0
            ]  # Get the first argument to await_only
            # The call should have been made with the creator function
            self.mock_psqlpy.connect.assert_called_once()

            # Check that the kwargs passed to psqlpy.connect include application_name
            # and exclude unsupported parameters
            call_kwargs = self.mock_psqlpy.connect.call_args[1]
            self.assertEqual(call_kwargs["application_name"], "test_app")
            self.assertEqual(call_kwargs["host"], "localhost")
            self.assertEqual(call_kwargs["port"], 5432)
            self.assertNotIn("server_settings", call_kwargs)
            self.assertNotIn("other_setting", call_kwargs)
            self.assertNotIn("unsupported_param", call_kwargs)

    def test_server_settings_without_application_name(self):
        """Test server_settings parameter handling without application_name"""
        with patch(
            "psqlpy_sqlalchemy.dbapi.AsyncAdapt_psqlpy_connection"
        ), patch("psqlpy_sqlalchemy.dbapi.await_only") as mock_await_only:
            mock_connection = Mock()
            mock_await_only.return_value = mock_connection

            # Test with server_settings not containing application_name
            server_settings = {"other_setting": "value"}

            self.adapt_dbapi.connect(
                host="localhost", server_settings=server_settings
            )

            # Check that no application_name was added
            call_kwargs = self.mock_psqlpy.connect.call_args[1]
            self.assertNotIn("application_name", call_kwargs)
            self.assertEqual(call_kwargs["host"], "localhost")
            self.assertNotIn("server_settings", call_kwargs)
            self.assertNotIn("other_setting", call_kwargs)

    def test_connect_without_server_settings(self):
        """Test connect method without server_settings"""
        with patch(
            "psqlpy_sqlalchemy.dbapi.AsyncAdapt_psqlpy_connection"
        ), patch("psqlpy_sqlalchemy.dbapi.await_only") as mock_await_only:
            mock_connection = Mock()
            mock_await_only.return_value = mock_connection

            self.adapt_dbapi.connect(host="localhost", port=5432)

            # Verify normal connection without server_settings
            call_kwargs = self.mock_psqlpy.connect.call_args[1]
            self.assertEqual(call_kwargs["host"], "localhost")
            self.assertEqual(call_kwargs["port"], 5432)
            self.assertNotIn("server_settings", call_kwargs)

    def test_parameter_filtering(self):
        """Test that unsupported parameters are filtered out"""
        with patch(
            "psqlpy_sqlalchemy.dbapi.AsyncAdapt_psqlpy_connection"
        ), patch("psqlpy_sqlalchemy.dbapi.await_only") as mock_await_only:
            mock_connection = Mock()
            mock_await_only.return_value = mock_connection

            self.adapt_dbapi.connect(
                host="localhost",
                port=5432,
                db_name="testdb",
                unsupported_param1="value1",
                unsupported_param2="value2",
            )

            # Check that only supported parameters are passed
            call_kwargs = self.mock_psqlpy.connect.call_args[1]
            self.assertEqual(call_kwargs["host"], "localhost")
            self.assertEqual(call_kwargs["port"], 5432)
            self.assertEqual(call_kwargs["db_name"], "testdb")
            self.assertNotIn("unsupported_param1", call_kwargs)
            self.assertNotIn("unsupported_param2", call_kwargs)


if __name__ == "__main__":
    unittest.main()
