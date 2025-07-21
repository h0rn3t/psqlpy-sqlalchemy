import psqlpy


class PsqlpyDBAPI:
    """DBAPI-compatible module interface for psqlpy"""

    # DBAPI 2.0 module attributes
    apilevel = "2.0"
    threadsafety = 2  # Threads may share the module and connections
    paramstyle = (
        "numeric_dollar"  # PostgreSQL uses $1, $2, etc. style parameters
    )

    # Exception hierarchy (DBAPI 2.0 standard)
    Warning = psqlpy.Error
    Error = psqlpy.Error
    InterfaceError = psqlpy.Error
    DatabaseError = psqlpy.Error
    DataError = psqlpy.Error
    OperationalError = psqlpy.Error
    IntegrityError = psqlpy.Error
    InternalError = psqlpy.Error
    ProgrammingError = psqlpy.Error
    NotSupportedError = psqlpy.Error

    # Type constructors
    def Date(self, year, month, day):
        """Construct a date value"""
        import datetime

        return datetime.date(year, month, day)

    def Time(self, hour, minute, second):
        """Construct a time value"""
        import datetime

        return datetime.time(hour, minute, second)

    def Timestamp(self, year, month, day, hour, minute, second):
        """Construct a timestamp value"""
        import datetime

        return datetime.datetime(year, month, day, hour, minute, second)

    def DateFromTicks(self, ticks):
        """Construct a date from ticks"""
        import datetime

        return datetime.date.fromtimestamp(ticks)

    def TimeFromTicks(self, ticks):
        """Construct a time from ticks"""
        import datetime

        dt = datetime.datetime.fromtimestamp(ticks)
        return dt.time()

    def TimestampFromTicks(self, ticks):
        """Construct a timestamp from ticks"""
        import datetime

        return datetime.datetime.fromtimestamp(ticks)

    def Binary(self, string):
        """Construct a binary value"""
        if isinstance(string, str):
            return string.encode("utf-8")
        return bytes(string)

    # Type objects for type comparison
    STRING = str
    BINARY = bytes
    NUMBER = (int, float)
    DATETIME = object  # datetime objects
    ROWID = int

    def connect(self, *args, **kwargs):
        """Create a connection - delegates to psqlpy.connect"""
        return psqlpy.connect(*args, **kwargs)
