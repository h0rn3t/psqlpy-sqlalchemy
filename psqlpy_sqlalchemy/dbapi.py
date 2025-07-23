import typing as t

from sqlalchemy.util.concurrency import await_only

from .connection import AsyncAdapt_psqlpy_connection


class PSQLPyAdaptDBAPI:
    def __init__(self, psqlpy: t.Any) -> None:
        self.psqlpy = psqlpy
        self.paramstyle = "numeric_dollar"

        # DBAPI 2.0 module attributes
        self.apilevel = "2.0"
        self.threadsafety = 2  # Threads may share the module and connections

        # Single reusable exception class for all error types
        _error_class = psqlpy.Error
        self.Warning = _error_class
        self.Error = _error_class
        self.InterfaceError = _error_class
        self.DatabaseError = _error_class
        self.DataError = _error_class
        self.OperationalError = _error_class
        self.IntegrityError = _error_class
        self.InternalError = _error_class
        self.ProgrammingError = _error_class
        self.NotSupportedError = _error_class

        for k, v in self.psqlpy.__dict__.items():
            if k != "connect":
                self.__dict__[k] = v

    def connect(
        self, *arg: t.Any, **kw: t.Any
    ) -> AsyncAdapt_psqlpy_connection:
        creator_fn = kw.pop("async_creator_fn", self.psqlpy.connect)

        # Handle server_settings parameter that SQLAlchemy might pass
        server_settings = kw.pop("server_settings", None)
        if server_settings and "application_name" in server_settings:
            kw["application_name"] = server_settings["application_name"]
            # Add other server_settings mappings as needed

        # Filter out any other unsupported parameters that SQLAlchemy might pass
        supported_params = frozenset(
            {
                "dsn",
                "username",
                "password",
                "host",
                "hosts",
                "port",
                "ports",
                "db_name",
                "target_session_attrs",
                "options",
                "application_name",
                "connect_timeout_sec",
                "connect_timeout_nanosec",
                "tcp_user_timeout_sec",
                "tcp_user_timeout_nanosec",
                "keepalives",
                "keepalives_idle_sec",
                "keepalives_idle_nanosec",
                "keepalives_interval_sec",
                "keepalives_interval_nanosec",
                "keepalives_retries",
                "load_balance_hosts",
                "max_db_pool_size",
                "conn_recycling_method",
                "ssl_mode",
                "ca_file",
            }
        )

        filtered_kw = {k: v for k, v in kw.items() if k in supported_params}

        return AsyncAdapt_psqlpy_connection(
            self, await_only(creator_fn(*arg, **filtered_kw))
        )


class PsqlpyDBAPI:
    """DBAPI-compatible module interface for psqlpy"""

    apilevel = "2.0"
    threadsafety = 2  # Threads may share the module and connections
    paramstyle = (
        "numeric_dollar"  # PostgreSQL uses $1, $2, etc. style parameters
    )

    def __init__(self) -> None:
        # Initialize with psqlpy module
        import psqlpy

        self._adapt_dbapi = PSQLPyAdaptDBAPI(psqlpy)

        # Single reusable exception class for all error types
        _error_class = psqlpy.Error
        self.Warning = _error_class
        self.Error = _error_class
        self.InterfaceError = _error_class
        self.DatabaseError = _error_class
        self.DataError = _error_class
        self.OperationalError = _error_class
        self.IntegrityError = _error_class
        self.InternalError = _error_class
        self.ProgrammingError = _error_class
        self.NotSupportedError = _error_class

    # Type constructors
    def Date(self, year: int, month: int, day: int) -> t.Any:
        """Construct a date value"""
        import datetime

        return datetime.date(year, month, day)

    def Time(self, hour: int, minute: int, second: int) -> t.Any:
        """Construct a time value"""
        import datetime

        return datetime.time(hour, minute, second)

    def Timestamp(
        self,
        year: int,
        month: int,
        day: int,
        hour: int,
        minute: int,
        second: int,
    ) -> t.Any:
        """Construct a timestamp value"""
        import datetime

        return datetime.datetime(year, month, day, hour, minute, second)

    def DateFromTicks(self, ticks: float) -> t.Any:
        """Construct a date from ticks"""
        import datetime

        return datetime.date.fromtimestamp(ticks)

    def TimeFromTicks(self, ticks: float) -> t.Any:
        """Construct a time from ticks"""
        import datetime

        dt = datetime.datetime.fromtimestamp(ticks)
        return dt.time()

    def TimestampFromTicks(self, ticks: float) -> t.Any:
        """Construct a timestamp from ticks"""
        import datetime

        return datetime.datetime.fromtimestamp(ticks)

    def Binary(self, string: t.Union[str, bytes]) -> bytes:
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

    def connect(
        self, *args: t.Any, **kwargs: t.Any
    ) -> AsyncAdapt_psqlpy_connection:
        """Create a connection - delegates to the adapted DBAPI"""
        return self._adapt_dbapi.connect(*args, **kwargs)
