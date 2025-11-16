#!/usr/bin/env python3
"""
Unit tests for psqlpy-sqlalchemy connection module
"""

import unittest
import uuid
from collections import deque
from unittest.mock import AsyncMock, Mock, patch

from psqlpy_sqlalchemy.connection import (
    AsyncAdapt_psqlpy_connection,
    AsyncAdapt_psqlpy_cursor,
    AsyncAdapt_psqlpy_ss_cursor,
    PGExecutionContext_psqlpy,
)


class TestPGExecutionContext(unittest.TestCase):
    """Test cases for PGExecutionContext_psqlpy"""

    def test_create_server_side_cursor(self):
        """Test server-side cursor creation"""
        # Mock the connection
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Create context by directly setting the attribute
        context = PGExecutionContext_psqlpy()
        context._dbapi_connection = mock_connection

        # Test server-side cursor creation
        result = context.create_server_side_cursor()

        mock_connection.cursor.assert_called_once_with(server_side=True)
        self.assertEqual(result, mock_cursor)


class TestAsyncAdaptPsqlpyCursor(unittest.TestCase):
    """Test cases for AsyncAdapt_psqlpy_cursor"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_adapt_connection = Mock()
        self.mock_connection = Mock()
        self.mock_adapt_connection._connection = self.mock_connection
        self.mock_adapt_connection._started = False
        self.mock_adapt_connection._connection_valid = True
        self.mock_adapt_connection.await_ = Mock()

        self.cursor = AsyncAdapt_psqlpy_cursor(self.mock_adapt_connection)

    def test_init(self):
        """Test cursor initialization"""
        self.assertEqual(
            self.cursor._adapt_connection, self.mock_adapt_connection
        )
        self.assertEqual(self.cursor._connection, self.mock_connection)
        self.assertEqual(self.cursor._arraysize, 1)
        self.assertIsNone(self.cursor._description)
        self.assertEqual(self.cursor._rowcount, -1)
        self.assertIsInstance(self.cursor._rows, deque)
        self.assertFalse(self.cursor.server_side)

    def test_process_parameters_none(self):
        """Test parameter processing with None"""
        result = self.cursor._process_parameters(None)
        self.assertIsNone(result)

    def test_process_parameters_dict(self):
        """Test parameter processing with dictionary"""
        test_uuid = uuid.uuid4()
        params = {
            "id": test_uuid,
            "name": "test",
            "uuid_str": str(test_uuid),
            "null_val": None,
        }

        result = self.cursor._process_parameters(params)

        self.assertIsInstance(result, dict)
        self.assertEqual(result["id"], test_uuid.bytes)
        self.assertEqual(result["name"], "test")
        self.assertEqual(result["uuid_str"], test_uuid.bytes)
        self.assertIsNone(result["null_val"])

    def test_process_parameters_list(self):
        """Test parameter processing with list"""
        test_uuid = uuid.uuid4()
        params = [test_uuid, "test", str(test_uuid), None]

        result = self.cursor._process_parameters(params)

        self.assertIsInstance(result, list)
        self.assertEqual(result[0], test_uuid.bytes)
        self.assertEqual(result[1], "test")
        self.assertEqual(result[2], test_uuid.bytes)
        self.assertIsNone(result[3])

    def test_process_parameters_invalid_uuid_string(self):
        """Test parameter processing with invalid UUID string"""
        params = {"invalid_uuid": "not-a-uuid"}

        result = self.cursor._process_parameters(params)

        self.assertEqual(result["invalid_uuid"], "not-a-uuid")

    def test_convert_named_params_no_params(self):
        """Test named parameter conversion with no parameters"""
        query = "SELECT * FROM table"
        result_query, result_params = (
            self.cursor._convert_named_params_with_casting(query, None)
        )

        self.assertEqual(result_query, query)
        self.assertIsNone(result_params)

    def test_convert_named_params_not_dict(self):
        """Test named parameter conversion with non-dict parameters"""
        query = "SELECT * FROM table WHERE id = $1"
        params = [123]

        result_query, result_params = (
            self.cursor._convert_named_params_with_casting(query, params)
        )

        self.assertEqual(result_query, query)
        self.assertEqual(result_params, params)

    def test_convert_named_params_no_matches(self):
        """Test named parameter conversion with no parameter matches"""
        query = "SELECT * FROM table"
        params = {"id": 123}

        result_query, result_params = (
            self.cursor._convert_named_params_with_casting(query, params)
        )

        self.assertEqual(result_query, query)
        self.assertEqual(result_params, params)

    def test_convert_named_params_missing_params(self):
        """Test named parameter conversion with missing parameters"""
        query = "SELECT * FROM table WHERE id = :id AND name = :name"
        params = {"id": 123}  # Missing 'name' parameter

        result_query, result_params = (
            self.cursor._convert_named_params_with_casting(query, params)
        )

        # Should return original query and params when parameters are missing
        self.assertEqual(result_query, query)
        self.assertEqual(result_params, params)

    def test_convert_named_params_success(self):
        """Test successful named parameter conversion"""
        query = "SELECT * FROM table WHERE id = :id AND name = :name"
        params = {"id": 123, "name": "test"}

        result_query, result_params = (
            self.cursor._convert_named_params_with_casting(query, params)
        )

        self.assertEqual(
            result_query, "SELECT * FROM table WHERE id = $1 AND name = $2"
        )
        self.assertEqual(result_params, [123, "test"])

    def test_convert_named_params_with_casting(self):
        """Test named parameter conversion with PostgreSQL casting"""
        query = "SELECT * FROM table WHERE id = :id::UUID"
        params = {"id": "123e4567-e89b-12d3-a456-426614174000"}

        result_query, result_params = (
            self.cursor._convert_named_params_with_casting(query, params)
        )

        self.assertEqual(
            result_query, "SELECT * FROM table WHERE id = $1::UUID"
        )
        self.assertEqual(
            result_params, ["123e4567-e89b-12d3-a456-426614174000"]
        )

    def test_properties(self):
        """Test cursor properties"""
        # Test description property
        self.assertIsNone(self.cursor.description)

        test_description = [("col1", None, None, None, None, None, None)]
        self.cursor._description = test_description
        self.assertEqual(self.cursor.description, test_description)

        # Test rowcount property
        self.assertEqual(self.cursor.rowcount, -1)

        self.cursor._rowcount = 5
        self.assertEqual(self.cursor.rowcount, 5)

        # Test arraysize property
        self.assertEqual(self.cursor.arraysize, 1)

        self.cursor.arraysize = 10
        self.assertEqual(self.cursor.arraysize, 10)

    @patch.object(
        AsyncAdapt_psqlpy_cursor, "_prepare_execute", new_callable=AsyncMock
    )
    def test_execute(self, mock_prepare_execute):
        """Test cursor execute method"""
        operation = "SELECT * FROM table"
        parameters = {"id": 123}

        # Execute is now synchronous and uses await_ internally
        self.cursor.execute(operation, parameters)

        mock_prepare_execute.assert_called_once_with(operation, parameters)

    @patch.object(
        AsyncAdapt_psqlpy_cursor, "_executemany", new_callable=AsyncMock
    )
    def test_executemany(self, mock_executemany):
        """Test cursor executemany method"""
        operation = "INSERT INTO table VALUES ($1, $2)"
        seq_of_parameters = [[1, "a"], [2, "b"]]

        # Executemany is now synchronous and uses await_ internally
        self.cursor.executemany(operation, seq_of_parameters)

        mock_executemany.assert_called_once_with(operation, seq_of_parameters)

    def test_setinputsizes(self):
        """Test setinputsizes method raises NotImplementedError"""
        with self.assertRaises(NotImplementedError):
            self.cursor.setinputsizes(10, 20)

    def test_process_parameters_single_value(self):
        """Test parameter processing with single value (not dict/list)"""
        test_uuid = uuid.uuid4()

        # Test with UUID
        result = self.cursor._process_parameters(test_uuid)
        self.assertEqual(result, test_uuid.bytes)

        # Test with string
        result = self.cursor._process_parameters("test_string")
        self.assertEqual(result, "test_string")

    @patch.object(
        AsyncAdapt_psqlpy_cursor, "_executemany", new_callable=AsyncMock
    )
    def test_executemany_coverage(self, mock_executemany):
        """Test executemany method for coverage"""
        operation = "INSERT INTO test VALUES ($1, $2)"
        seq_of_parameters = [[1, "a"], [2, "b"]]

        # Executemany is now synchronous and uses await_ internally
        self.cursor.executemany(operation, seq_of_parameters)

        mock_executemany.assert_called_once_with(operation, seq_of_parameters)

    # UPDATE operation tests
    @patch.object(
        AsyncAdapt_psqlpy_cursor, "_prepare_execute", new_callable=AsyncMock
    )
    def test_execute_update_basic(self, mock_prepare_execute):
        """Test basic UPDATE operation with execute method"""
        operation = "UPDATE users SET name = 'John' WHERE id = 1"

        self.cursor.execute(operation)

        mock_prepare_execute.assert_called_once_with(operation, None)

    @patch.object(
        AsyncAdapt_psqlpy_cursor, "_prepare_execute", new_callable=AsyncMock
    )
    def test_execute_update_with_named_parameters(self, mock_prepare_execute):
        """Test UPDATE operation with named parameters"""
        operation = (
            "UPDATE users SET name = :name, email = :email WHERE id = :id"
        )
        parameters = {"name": "John Doe", "email": "john@example.com", "id": 1}

        self.cursor.execute(operation, parameters)

        mock_prepare_execute.assert_called_once_with(operation, parameters)

    @patch.object(
        AsyncAdapt_psqlpy_cursor, "_prepare_execute", new_callable=AsyncMock
    )
    def test_execute_update_with_positional_parameters(
        self, mock_prepare_execute
    ):
        """Test UPDATE operation with positional parameters"""
        operation = "UPDATE users SET name = $1, email = $2 WHERE id = $3"
        parameters = ["John Doe", "john@example.com", 1]

        self.cursor.execute(operation, parameters)

        mock_prepare_execute.assert_called_once_with(operation, parameters)

    @patch.object(
        AsyncAdapt_psqlpy_cursor, "_prepare_execute", new_callable=AsyncMock
    )
    def test_execute_update_multiple_columns(self, mock_prepare_execute):
        """Test UPDATE operation with multiple columns"""
        operation = "UPDATE users SET name = :name, email = :email, age = :age, updated_at = NOW() WHERE id = :id"
        parameters = {
            "name": "Jane Smith",
            "email": "jane@example.com",
            "age": 30,
            "id": 2,
        }

        self.cursor.execute(operation, parameters)

        mock_prepare_execute.assert_called_once_with(operation, parameters)

    @patch.object(
        AsyncAdapt_psqlpy_cursor, "_prepare_execute", new_callable=AsyncMock
    )
    def test_execute_update_with_where_clause(self, mock_prepare_execute):
        """Test UPDATE operation with complex WHERE clause"""
        operation = "UPDATE users SET status = :status WHERE age > :min_age AND created_at < :date"
        parameters = {"status": "active", "min_age": 18, "date": "2023-01-01"}

        self.cursor.execute(operation, parameters)

        mock_prepare_execute.assert_called_once_with(operation, parameters)

    @patch.object(
        AsyncAdapt_psqlpy_cursor, "_executemany", new_callable=AsyncMock
    )
    def test_executemany_update_operations(self, mock_executemany):
        """Test UPDATE operations with executemany method"""
        operation = "UPDATE users SET name = $1, email = $2 WHERE id = $3"
        seq_of_parameters = [
            ["John Doe", "john@example.com", 1],
            ["Jane Smith", "jane@example.com", 2],
            ["Bob Johnson", "bob@example.com", 3],
        ]

        self.cursor.executemany(operation, seq_of_parameters)

        mock_executemany.assert_called_once_with(operation, seq_of_parameters)

    @patch.object(
        AsyncAdapt_psqlpy_cursor, "_executemany", new_callable=AsyncMock
    )
    def test_executemany_update_with_dict_parameters(self, mock_executemany):
        """Test UPDATE operations with executemany using dict parameters"""
        operation = "UPDATE users SET name = :name WHERE id = :id"
        seq_of_parameters = [
            {"name": "John Updated", "id": 1},
            {"name": "Jane Updated", "id": 2},
        ]

        self.cursor.executemany(operation, seq_of_parameters)

        mock_executemany.assert_called_once_with(operation, seq_of_parameters)

    @patch.object(
        AsyncAdapt_psqlpy_cursor, "_prepare_execute", new_callable=AsyncMock
    )
    def test_execute_update_with_uuid_parameter(self, mock_prepare_execute):
        """Test UPDATE operation with UUID parameter (tests the async fix)"""
        test_uuid = uuid.uuid4()
        operation = "UPDATE users SET profile_id = :profile_id WHERE id = :id"
        parameters = {"profile_id": test_uuid, "id": 1}

        self.cursor.execute(operation, parameters)

        mock_prepare_execute.assert_called_once_with(operation, parameters)

    @patch.object(
        AsyncAdapt_psqlpy_cursor, "_prepare_execute", new_callable=AsyncMock
    )
    def test_execute_update_async_greenlet_fix(self, mock_prepare_execute):
        """Test that UPDATE operations work with the async/greenlet fix"""
        # This test specifically verifies that the async fix works for UPDATE operations
        # that were causing the original greenlet switching issue
        operation = "UPDATE test_table SET name = :name WHERE id = :id"
        parameters = {"name": "test_update", "id": 1}

        # This should not raise any greenlet-related errors
        self.cursor.execute(operation, parameters)

        mock_prepare_execute.assert_called_once_with(operation, parameters)

        # Verify that the method was called without await_only issues
        self.assertTrue(mock_prepare_execute.called)

    @patch.object(
        AsyncAdapt_psqlpy_cursor, "_prepare_execute", new_callable=AsyncMock
    )
    def test_execute_update_with_null_values(self, mock_prepare_execute):
        """Test UPDATE operation with NULL values"""
        operation = (
            "UPDATE users SET email = :email, phone = :phone WHERE id = :id"
        )
        parameters = {"email": None, "phone": None, "id": 1}

        self.cursor.execute(operation, parameters)

        mock_prepare_execute.assert_called_once_with(operation, parameters)


class TestAsyncAdaptPsqlpySSCursor(unittest.TestCase):
    """Test cases for AsyncAdapt_psqlpy_ss_cursor"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_adapt_connection = Mock()
        self.mock_connection = Mock()
        self.mock_adapt_connection._connection = self.mock_connection
        self.mock_adapt_connection.await_ = Mock()

        self.ss_cursor = AsyncAdapt_psqlpy_ss_cursor(
            self.mock_adapt_connection
        )

    def test_init(self):
        """Test server-side cursor initialization"""
        self.assertEqual(
            self.ss_cursor._adapt_connection, self.mock_adapt_connection
        )
        self.assertEqual(self.ss_cursor._connection, self.mock_connection)
        self.assertIsNone(self.ss_cursor._cursor)
        self.assertFalse(self.ss_cursor._closed)

    def test_convert_result_none(self):
        """Test result conversion with None result"""
        result = self.ss_cursor._convert_result(None)
        self.assertEqual(result, ())

    def test_convert_result_success(self):
        """Test successful result conversion"""
        # Mock QueryResult
        mock_result = Mock()
        Mock()
        mock_result.row_factory.return_value = [
            [("col1", "value1"), ("col2", "value2")],
            [("col1", "value3"), ("col2", "value4")],
        ]

        result = self.ss_cursor._convert_result(mock_result)

        expected = (("value1", "value2"), ("value3", "value4"))
        self.assertEqual(result, expected)

    def test_convert_result_exception(self):
        """Test result conversion with exception"""
        mock_result = Mock()
        mock_result.row_factory.side_effect = Exception("Conversion error")

        result = self.ss_cursor._convert_result(mock_result)

        self.assertEqual(result, ())

    def test_close(self):
        """Test cursor close method"""
        mock_cursor = Mock()
        self.ss_cursor._cursor = mock_cursor

        self.ss_cursor.close()

        mock_cursor.close.assert_called_once()
        self.assertIsNone(self.ss_cursor._cursor)
        self.assertTrue(self.ss_cursor._closed)

    def test_close_already_closed(self):
        """Test closing already closed cursor"""
        self.ss_cursor._closed = True
        self.ss_cursor._cursor = None

        # Should not raise exception
        self.ss_cursor.close()

    def test_close_with_exception(self):
        """Test close method with exception"""
        mock_cursor = Mock()
        mock_cursor.close.side_effect = Exception("Close error")
        self.ss_cursor._cursor = mock_cursor

        # Should not raise exception
        self.ss_cursor.close()

        self.assertIsNone(self.ss_cursor._cursor)
        self.assertTrue(self.ss_cursor._closed)

    def test_fetchone_closed(self):
        """Test fetchone on closed cursor"""
        self.ss_cursor._closed = True

        result = self.ss_cursor.fetchone()

        self.assertIsNone(result)

    def test_fetchone_no_cursor(self):
        """Test fetchone with no cursor"""
        self.ss_cursor._cursor = None

        result = self.ss_cursor.fetchone()

        self.assertIsNone(result)

    def test_fetchone_success(self):
        """Test successful fetchone"""
        mock_cursor = Mock()
        mock_result = Mock()
        mock_result.row_factory.return_value = [[("col1", "value1")]]

        self.ss_cursor._cursor = mock_cursor
        self.ss_cursor._adapt_connection.await_.return_value = mock_result

        result = self.ss_cursor.fetchone()

        self.assertEqual(result, ("value1",))

    def test_fetchone_exception(self):
        """Test fetchone with exception"""
        mock_cursor = Mock()
        self.ss_cursor._cursor = mock_cursor
        self.ss_cursor._adapt_connection.await_.side_effect = Exception(
            "Fetch error"
        )

        result = self.ss_cursor.fetchone()

        self.assertIsNone(result)

    def test_fetchmany_closed(self):
        """Test fetchmany on closed cursor"""
        self.ss_cursor._closed = True

        result = self.ss_cursor.fetchmany()

        self.assertEqual(result, [])

    def test_fetchmany_success(self):
        """Test successful fetchmany"""
        mock_cursor = Mock()
        mock_result = Mock()
        mock_result.row_factory.return_value = [
            [("col1", "value1")],
            [("col1", "value2")],
        ]

        self.ss_cursor._cursor = mock_cursor
        self.ss_cursor._adapt_connection.await_.return_value = mock_result

        result = self.ss_cursor.fetchmany(size=2)

        mock_cursor.fetchmany.assert_called_once_with(size=2)
        self.assertEqual(result, [("value1",), ("value2",)])

    def test_fetchall_success(self):
        """Test successful fetchall"""
        mock_cursor = Mock()
        mock_result = Mock()
        mock_result.row_factory.return_value = [
            [("col1", "value1")],
            [("col1", "value2")],
        ]

        self.ss_cursor._cursor = mock_cursor
        self.ss_cursor._adapt_connection.await_.return_value = mock_result

        result = self.ss_cursor.fetchall()

        mock_cursor.fetchall.assert_called_once()
        self.assertEqual(result, [("value1",), ("value2",)])

    def test_iter_closed(self):
        """Test iteration on closed cursor"""
        self.ss_cursor._closed = True

        result = list(self.ss_cursor.__iter__())

        self.assertEqual(result, [])

    def test_iter_success(self):
        """Test successful iteration"""
        mock_cursor = Mock()
        mock_iterator = Mock()

        # Mock async iteration properly
        mock_iterator.__anext__ = Mock(
            side_effect=[
                Mock(row_factory=lambda x: [[("col1", "value1")]]),
                Mock(row_factory=lambda x: [[("col1", "value2")]]),
                StopAsyncIteration(),
            ]
        )

        # Create a proper mock for __aiter__
        mock_cursor.__aiter__ = Mock(return_value=mock_iterator)
        self.ss_cursor._cursor = mock_cursor
        self.ss_cursor._adapt_connection.await_.side_effect = [
            Mock(row_factory=lambda x: [[("col1", "value1")]]),
            Mock(row_factory=lambda x: [[("col1", "value2")]]),
            StopAsyncIteration(),
        ]

        result = list(self.ss_cursor.__iter__())

        self.assertEqual(result, [("value1",), ("value2",)])


class TestAsyncAdaptPsqlpyConnection(unittest.TestCase):
    """Test cases for AsyncAdapt_psqlpy_connection"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_dbapi = Mock()
        self.mock_connection = Mock()

        self.connection = AsyncAdapt_psqlpy_connection(
            self.mock_dbapi, self.mock_connection
        )

    def test_init(self):
        """Test connection initialization"""
        self.assertEqual(self.connection.dbapi, self.mock_dbapi)
        self.assertEqual(self.connection._connection, self.mock_connection)
        self.assertFalse(self.connection._started)
        self.assertTrue(self.connection._connection_valid)

    def test_set_isolation_level(self):
        """Test set_isolation_level method"""
        # This method is a no-op, should not raise exception
        self.connection.set_isolation_level("READ_COMMITTED")

    def test_is_valid_true(self):
        """Test is_valid method when connection is valid"""
        self.assertTrue(self.connection.is_valid())

    def test_is_valid_false(self):
        """Test is_valid method when connection is invalid"""
        self.connection._connection_valid = False
        self.assertFalse(self.connection.is_valid())

    def test_close(self):
        """Test close method"""
        # This method is a no-op, should not raise exception
        self.connection.close()

    def test_cursor_regular(self):
        """Test cursor creation (regular cursor)"""
        cursor = self.connection.cursor(server_side=False)

        self.assertIsInstance(cursor, AsyncAdapt_psqlpy_cursor)

    def test_cursor_server_side(self):
        """Test cursor creation (server-side cursor)"""
        cursor = self.connection.cursor(server_side=True)

        self.assertIsInstance(cursor, AsyncAdapt_psqlpy_ss_cursor)

    @patch("psqlpy_sqlalchemy.connection.await_only")
    def test_rollback_with_transaction(self, mock_await_only):
        """Test rollback with active transaction"""
        mock_transaction = Mock()
        self.connection._transaction = mock_transaction

        self.connection.rollback()

        mock_await_only.assert_called_once_with(mock_transaction.rollback())
        self.assertIsNone(self.connection._transaction)
        self.assertFalse(self.connection._started)

    @patch("psqlpy_sqlalchemy.connection.await_only")
    def test_rollback_without_transaction(self, mock_await_only):
        """Test rollback without active transaction"""
        self.connection._transaction = None

        self.connection.rollback()

        mock_await_only.assert_called_once_with(
            self.mock_connection.rollback()
        )

    @patch("psqlpy_sqlalchemy.connection.await_only")
    def test_rollback_with_exception(self, mock_await_only):
        """Test rollback with exception"""
        mock_transaction = Mock()
        mock_transaction.rollback.side_effect = Exception("Rollback error")
        self.connection._transaction = mock_transaction
        mock_await_only.side_effect = Exception("Rollback error")

        # Should not raise exception
        self.connection.rollback()

        self.assertIsNone(self.connection._transaction)
        self.assertFalse(self.connection._started)
        self.assertFalse(self.connection._connection_valid)

    @patch("psqlpy_sqlalchemy.connection.await_only")
    def test_commit_with_transaction(self, mock_await_only):
        """Test commit with active transaction"""
        mock_transaction = Mock()
        self.connection._transaction = mock_transaction

        self.connection.commit()

        mock_await_only.assert_called_once_with(mock_transaction.commit())
        self.assertIsNone(self.connection._transaction)
        self.assertFalse(self.connection._started)

    @patch("psqlpy_sqlalchemy.connection.await_only")
    def test_commit_without_transaction(self, mock_await_only):
        """Test commit without active transaction"""
        self.connection._transaction = None

        self.connection.commit()

        mock_await_only.assert_called_once_with(self.mock_connection.commit())

    @patch("psqlpy_sqlalchemy.connection.await_only")
    def test_commit_with_exception(self, mock_await_only):
        """Test commit with exception"""
        mock_transaction = Mock()
        self.connection._transaction = mock_transaction
        mock_await_only.side_effect = [
            RuntimeError("Commit error"),
            None,
        ]  # commit fails, rollback succeeds

        with self.assertRaises(RuntimeError):
            self.connection.commit()

        self.assertFalse(self.connection._connection_valid)

    @patch("psqlpy_sqlalchemy.connection.await_only")
    @patch("time.time")
    def test_ping_success(self, mock_time, mock_await_only):
        """Test successful ping"""
        mock_time.return_value = 100
        self.connection._last_ping_time = 0  # Force ping

        result = self.connection.ping()

        self.assertTrue(result)
        self.assertTrue(self.connection._connection_valid)
        self.assertEqual(self.connection._last_ping_time, 100)
        mock_await_only.assert_called_once_with(
            self.mock_connection.execute("SELECT 1")
        )

    @patch("psqlpy_sqlalchemy.connection.await_only")
    @patch("time.time")
    def test_ping_failure(self, mock_time, mock_await_only):
        """Test ping failure"""
        mock_time.return_value = 100
        self.connection._last_ping_time = 0  # Force ping
        mock_await_only.side_effect = Exception("Connection error")

        result = self.connection.ping()

        self.assertFalse(result)
        self.assertFalse(self.connection._connection_valid)

    @patch("time.time")
    def test_ping_recent(self, mock_time):
        """Test ping when recently pinged"""
        mock_time.return_value = 100
        self.connection._last_ping_time = 80  # Less than 30 seconds ago

        result = self.connection.ping()

        self.assertTrue(result)  # Should return cached result


class TestAsyncAdaptPsqlpyCursorAsync(unittest.TestCase):
    """Test cases for async methods in AsyncAdapt_psqlpy_cursor"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_adapt_connection = Mock()
        self.mock_connection = AsyncMock()
        self.mock_adapt_connection._connection = self.mock_connection
        self.mock_adapt_connection._started = False
        self.mock_adapt_connection._transaction = None
        self.mock_adapt_connection._performance_stats = {
            "queries_executed": 0,
            "connection_errors": 0,
        }
        self.mock_adapt_connection._connection_valid = True

        self.cursor = AsyncAdapt_psqlpy_cursor(self.mock_adapt_connection)

    async def test_prepare_execute_success(self):
        """Test successful _prepare_execute"""
        # Mock prepared statement
        mock_prepared_stmt = Mock()
        mock_prepared_stmt.columns.return_value = [
            Mock(name="col1", table_oid=123),
            Mock(name="col2", table_oid=456),
        ]
        mock_result = Mock()
        mock_result.row_factory.return_value = [
            [("col1", "value1"), ("col2", "value2")],
            [("col1", "value3"), ("col2", "value4")],
        ]
        mock_prepared_stmt.execute.return_value = mock_result

        self.mock_connection.prepare.return_value = mock_prepared_stmt

        await self.cursor._prepare_execute("SELECT * FROM test", {"id": 123})

        self.mock_connection.prepare.assert_called_once()
        self.assertIsNotNone(self.cursor._description)
        self.assertEqual(len(self.cursor._rows), 2)
        self.assertEqual(self.cursor._rowcount, 2)
        self.assertEqual(
            self.mock_adapt_connection._performance_stats["queries_executed"],
            1,
        )

    async def test_prepare_execute_server_side(self):
        """Test _prepare_execute with server-side cursor"""
        self.cursor.server_side = True
        mock_prepared_stmt = Mock()
        mock_prepared_stmt.columns.return_value = []
        mock_cursor = AsyncMock()

        self.mock_connection.prepare.return_value = mock_prepared_stmt
        self.mock_connection.cursor.return_value = mock_cursor

        await self.cursor._prepare_execute("SELECT * FROM test", [123])

        self.mock_connection.cursor.assert_called_once()
        mock_cursor.start.assert_called_once()
        self.assertEqual(self.cursor._rowcount, -1)

    async def test_prepare_execute_with_exception(self):
        """Test _prepare_execute with exception"""
        self.mock_connection.prepare.side_effect = RuntimeError(
            "Connection error"
        )

        with self.assertRaises(RuntimeError):
            await self.cursor._prepare_execute("SELECT * FROM test")

        self.assertIsNone(self.cursor._description)
        self.assertEqual(self.cursor._rowcount, -1)
        self.assertEqual(len(self.cursor._rows), 0)
        self.assertEqual(
            self.mock_adapt_connection._performance_stats["connection_errors"],
            1,
        )
        self.assertFalse(self.mock_adapt_connection._connection_valid)

    async def test_executemany_async(self):
        """Test _executemany method"""
        operation = "INSERT INTO test VALUES ($1, $2)"
        seq_of_parameters = [[1, "a"], [2, "b"]]

        await self.cursor._executemany(operation, seq_of_parameters)

        self.mock_connection.execute_many.assert_called_once_with(
            operation, seq_of_parameters, True
        )
        self.assertIsNone(self.cursor._description)

    def test_convert_named_params_casting_error(self):
        """Test named parameter conversion with casting error"""
        query = "SELECT * FROM test WHERE id = :id::UUID"
        params = {"id": "test"}

        # Test the actual method - it should handle casting parameters correctly
        converted_query, converted_params = (
            self.cursor._convert_named_params_with_casting(query, params)
        )

        # Should convert successfully
        self.assertEqual(
            converted_query, "SELECT * FROM test WHERE id = $1::UUID"
        )
        self.assertEqual(converted_params, ["test"])


class TestAsyncAdaptPsqlpyConnectionAsync(unittest.TestCase):
    """Test cases for async methods in AsyncAdapt_psqlpy_connection"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_dbapi = Mock()
        self.mock_connection = AsyncMock()

        self.connection = AsyncAdapt_psqlpy_connection(
            self.mock_dbapi, self.mock_connection
        )

    async def test_start_transaction_success(self):
        """Test successful transaction start"""
        mock_transaction = AsyncMock()
        self.mock_connection.transaction.return_value = mock_transaction

        await self.connection._start_transaction()

        self.mock_connection.transaction.assert_called_once()
        mock_transaction.begin.assert_called_once()
        self.assertEqual(self.connection._transaction, mock_transaction)
        self.assertTrue(self.connection._started)

    async def test_start_transaction_already_started(self):
        """Test transaction start when already started"""
        mock_transaction = Mock()
        self.connection._transaction = mock_transaction

        await self.connection._start_transaction()

        # Should not create new transaction
        self.mock_connection.transaction.assert_not_called()
        self.assertEqual(self.connection._transaction, mock_transaction)

    async def test_start_transaction_with_exception(self):
        """Test transaction start with exception"""
        self.mock_connection.transaction.side_effect = RuntimeError(
            "Transaction error"
        )

        with self.assertRaises(RuntimeError):
            await self.connection._start_transaction()

        self.assertIsNone(self.connection._transaction)
        self.assertFalse(self.connection._started)


class TestAsyncAdaptPsqlpySSCursorCoverage(unittest.TestCase):
    """Additional tests for server-side cursor coverage"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_adapt_connection = Mock()
        self.mock_connection = Mock()
        self.mock_adapt_connection._connection = self.mock_connection
        self.mock_adapt_connection.await_ = Mock()

        self.ss_cursor = AsyncAdapt_psqlpy_ss_cursor(
            self.mock_adapt_connection
        )

    def test_fetchmany_with_size_none(self):
        """Test fetchmany when size is None (uses arraysize)"""
        mock_cursor = Mock()
        mock_result = Mock()
        mock_result.row_factory = lambda x: [[("col1", "value1")]]

        self.ss_cursor._cursor = mock_cursor
        self.ss_cursor.arraysize = 5
        self.mock_adapt_connection.await_.return_value = mock_result

        result = self.ss_cursor.fetchmany(size=None)

        # Should use arraysize when size is None
        mock_cursor.fetchmany.assert_called_with(size=5)
        self.assertEqual(result, [("value1",)])

    def test_convert_result_exception(self):
        """Test _convert_result with exception"""
        mock_result = Mock()
        mock_result.row_factory.side_effect = Exception("Conversion error")

        result = self.ss_cursor._convert_result(mock_result)

        # Should return empty tuple on exception
        self.assertEqual(result, ())

    def test_fetchone_exception(self):
        """Test fetchone with exception"""
        mock_cursor = Mock()
        self.ss_cursor._cursor = mock_cursor
        self.mock_adapt_connection.await_.side_effect = Exception(
            "Fetch error"
        )

        result = self.ss_cursor.fetchone()

        # Should return None on exception
        self.assertIsNone(result)

    def test_fetchmany_exception(self):
        """Test fetchmany with exception"""
        mock_cursor = Mock()
        self.ss_cursor._cursor = mock_cursor
        self.mock_adapt_connection.await_.side_effect = Exception(
            "Fetch error"
        )

        result = self.ss_cursor.fetchmany()

        # Should return empty list on exception
        self.assertEqual(result, [])

    def test_fetchall_exception(self):
        """Test fetchall with exception"""
        mock_cursor = Mock()
        self.ss_cursor._cursor = mock_cursor
        self.mock_adapt_connection.await_.side_effect = Exception(
            "Fetch error"
        )

        result = self.ss_cursor.fetchall()

        # Should return empty list on exception
        self.assertEqual(result, [])


class TestAsyncAdaptPsqlpyConnectionCoverage(unittest.TestCase):
    """Additional tests for connection coverage"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_dbapi = Mock()
        self.mock_connection = Mock()
        self.connection = AsyncAdapt_psqlpy_connection(
            self.mock_dbapi, self.mock_connection
        )

    def test_ping_recent(self):
        """Test ping when recently pinged (within 30 seconds)"""
        import time

        # Set last ping time to recent
        self.connection._last_ping_time = time.time() - 10  # 10 seconds ago
        self.connection._connection_valid = True

        result = self.connection.ping()

        # Should return cached result without executing query
        self.assertTrue(result)
        self.mock_connection.execute.assert_not_called()

    def test_ping_exception(self):
        """Test ping with exception"""
        import time

        # Set last ping time to old
        self.connection._last_ping_time = time.time() - 60  # 60 seconds ago

        with patch("psqlpy_sqlalchemy.connection.await_only") as mock_await:
            mock_await.side_effect = Exception("Connection error")

            result = self.connection.ping()

            self.assertFalse(result)
            self.assertFalse(self.connection._connection_valid)


if __name__ == "__main__":
    unittest.main()
