from typing import List, Optional, Tuple, Union

import psqlpy


class PsqlpyCursor:
    """DBAPI-compatible cursor wrapper for psqlpy"""

    def __init__(self, connection: "PsqlpyConnection"):
        self.connection = connection
        self._psqlpy_connection = connection._psqlpy_connection
        self._result = None
        self._rows = None
        self._row_index = 0
        self.rowcount = -1
        self.description = None
        self.arraysize = 1

    def execute(
        self, query: str, parameters: Optional[Union[dict, list, tuple]] = None
    ):
        """Execute a query with optional parameters"""
        try:
            if parameters is None:
                query_result = self._psqlpy_connection.fetch(query)
            else:
                if isinstance(parameters, (list, tuple)):
                    param_dict = {
                        f"param_{i}": val for i, val in enumerate(parameters)
                    }
                    query = self._convert_positional_to_named(
                        query, len(parameters)
                    )
                    query_result = self._psqlpy_connection.fetch(
                        query, param_dict
                    )
                else:
                    query_result = self._psqlpy_connection.fetch(
                        query, parameters
                    )

            # Process the result - call .result() on the QueryResult object
            if query_result:
                self._rows = query_result.result()
                self.rowcount = len(self._rows) if self._rows else 0
                self._row_index = 0

                # Set description (column metadata)
                if self._rows and len(self._rows) > 0:
                    first_row = self._rows[0]
                    if isinstance(first_row, dict):
                        self.description = [
                            (name, None, None, None, None, None, None)
                            for name in first_row.keys()
                        ]
                    elif isinstance(first_row, (list, tuple)):
                        self.description = [
                            (f"column_{i}", None, None, None, None, None, None)
                            for i in range(len(first_row))
                        ]
            else:
                self._rows = []  # type: ignore
                self.rowcount = 0
                self.description = None

        except Exception as e:
            raise self._convert_exception(e)

    def executemany(
        self, query: str, parameters_list: List[Union[dict, list, tuple]]
    ):
        """Execute a query multiple times with different parameters"""
        try:
            total_rowcount = 0
            for parameters in parameters_list:
                self.execute(query, parameters)
                if self.rowcount > 0:
                    total_rowcount += self.rowcount

            self.rowcount = total_rowcount
            self._rows = []  # executemany typically doesn't return results
            self.description = None

        except Exception as e:
            raise self._convert_exception(e)

    def fetchone(self) -> Optional[Tuple]:
        """Fetch the next row"""
        if not self._rows or self._row_index >= len(self._rows):
            return None

        row = self._rows[self._row_index]
        self._row_index += 1

        # Convert to tuple if it's a dict
        if isinstance(row, dict):
            return tuple(row.values())
        elif isinstance(row, (list, tuple)):
            return tuple(row)
        else:
            return (row,)

    def fetchmany(self, size: Optional[int] = None) -> List[Tuple]:
        """Fetch multiple rows"""
        if size is None:
            size = self.arraysize

        results = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            results.append(row)

        return results

    def fetchall(self) -> List[Tuple]:
        """Fetch all remaining rows"""
        results = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            results.append(row)

        return results

    def close(self):
        """Close the cursor"""
        self._result = None
        self._rows = None
        self._row_index = 0
        self.rowcount = -1
        self.description = None

    def _convert_positional_to_named(
        self, query: str, param_count: int
    ) -> str:
        "Convert positional parameters (?) to named parameters (%(param_N)s)"
        result = query
        for i in range(param_count):
            result = result.replace("?", f"%(param_{i})s", 1)
        return result

    def _convert_exception(self, e):
        """Convert psqlpy exceptions to DBAPI exceptions"""
        # In a full implementation, you'd map specific error types
        return e


class PsqlpyConnection:
    """DBAPI-compatible connection wrapper for psqlpy"""

    def __init__(self, psqlpy_connection):
        self._psqlpy_connection = psqlpy_connection
        self._closed = False
        self._autocommit = True  # PostgreSQL default
        self._in_transaction = False

    def cursor(self):
        """Create a new cursor"""
        if self._closed:
            raise psqlpy.Error("Connection is closed")
        return PsqlpyCursor(self)

    def commit(self):
        """Commit the current transaction"""
        if self._closed:
            raise psqlpy.Error("Connection is closed")

        # psqlpy handles transactions differently
        # If we're in a transaction, we need to commit it
        if self._in_transaction:
            try:
                # This is a simplified approach - in practice you'd track
                # transactions better
                cursor = self.cursor()
                cursor.execute("COMMIT")
                self._in_transaction = False
            except Exception as e:
                raise self._convert_exception(e)

    def rollback(self):
        """Rollback the current transaction"""
        if self._closed:
            raise psqlpy.Error("Connection is closed")

        if self._in_transaction:
            try:
                cursor = self.cursor()
                cursor.execute("ROLLBACK")
                self._in_transaction = False
            except Exception as e:
                raise self._convert_exception(e)

    def close(self):
        """Close the connection"""
        if not self._closed:
            try:
                self._psqlpy_connection.close()
            except Exception:
                pass  # Ignore errors on close
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()
        self.close()

    @property
    def autocommit(self):
        """Get autocommit mode"""
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        """Set autocommit mode"""
        self._autocommit = value
        # In a full implementation, you'd configure the underlying connection

    def _convert_exception(self, e):
        """Convert psqlpy exceptions to DBAPI exceptions"""
        return e
