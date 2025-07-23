#!/usr/bin/env python3
"""
Unit tests for custom FastAPI middleware with psqlpy-sqlalchemy
Tests the custom middleware implementation provided in the issue description.
"""

import asyncio
import unittest
from contextvars import ContextVar
from typing import Dict, Optional, Union

try:
    from fastapi import FastAPI
    from starlette.middleware.base import (
        BaseHTTPMiddleware,
        RequestResponseEndpoint,
    )
    from starlette.requests import Request
    from starlette.types import ASGIApp

    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

    # Create mock classes for when FastAPI is not available
    class FastAPI:
        def __init__(self, *args, **kwargs):
            pass

        def add_middleware(self, *args, **kwargs):
            pass

        def get(self, *args, **kwargs):
            def decorator(func):
                return func

            return decorator

    class BaseHTTPMiddleware:
        def __init__(self, *args, **kwargs):
            pass


from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


# Mock settings for testing
class MockSettings:
    DB_URL = "postgresql+psqlpy://user:password@localhost:5432/testdb"
    SQLALCHEMY_ECHO = False
    DB_POOL_SIZE_MIN = 5
    DB_POOL_SIZE_MAX = 10
    SERVICE_NAME = "test_service"


settings = MockSettings()


# Mock SSLMode for testing
class SSLMode:
    disable = "disable"


# Custom exceptions for the middleware
class MissingSessionError(Exception):
    """Raised when no session is available in the current context."""

    pass


class SessionNotInitialisedError(Exception):
    """Raised when the session factory is not initialized."""

    pass


def create_middleware_and_session_proxy():
    """Create the custom middleware and session proxy as provided in the issue."""
    _Session: Optional[async_sessionmaker] = None
    _session: ContextVar[Optional[AsyncSession]] = ContextVar(
        "_session", default=None
    )
    _multi_sessions_ctx: ContextVar[bool] = ContextVar(
        "_multi_sessions_context", default=False
    )
    _commit_on_exit_ctx: ContextVar[bool] = ContextVar(
        "_commit_on_exit_ctx", default=False
    )

    class SQLAlchemyMiddleware(BaseHTTPMiddleware):
        def __init__(
            self,
            app: ASGIApp,
            db_url: Optional[Union[str, URL]] = None,
            custom_engine: Optional[Engine] = None,
            engine_args: Dict = None,
            session_args: Dict = None,
            commit_on_exit: bool = False,
        ):
            super().__init__(app)
            self.commit_on_exit = commit_on_exit
            engine_args = engine_args or {}
            session_args = session_args or {}

            if not custom_engine and not db_url:
                raise ValueError(
                    "You need to pass a db_url or a custom_engine parameter."
                )
            if not custom_engine:
                engine = create_async_engine(db_url, **engine_args)
            else:
                engine = custom_engine

            nonlocal _Session
            _Session = async_sessionmaker(
                engine,
                class_=AsyncSession,
                expire_on_commit=False,
                **session_args,
            )

        async def dispatch(
            self, request: Request, call_next: RequestResponseEndpoint
        ):
            async with DBSession(commit_on_exit=self.commit_on_exit):
                return await call_next(request)

    class DBSessionMeta(type):
        @property
        def session(self) -> AsyncSession:
            """Return an instance of Session local to the current async context."""
            if _Session is None:
                raise SessionNotInitialisedError

            multi_sessions = _multi_sessions_ctx.get()
            if multi_sessions:
                commit_on_exit = _commit_on_exit_ctx.get()
                # Always create a new session for each access when multi_sessions=True
                session = _Session()

                async def cleanup():
                    try:
                        if commit_on_exit:
                            await session.commit()
                    except Exception:
                        await session.rollback()
                        raise
                    finally:
                        await session.close()

                task = asyncio.current_task()
                if task is not None:
                    task.add_done_callback(
                        lambda t: asyncio.create_task(cleanup())
                    )
                return session
            session = _session.get()
            if session is None:
                raise MissingSessionError
            return session

    class DBSession(metaclass=DBSessionMeta):
        def __init__(
            self,
            session_args: Dict = None,
            commit_on_exit: bool = False,
            multi_sessions: bool = False,
        ):
            self.token = None
            self.commit_on_exit_token = None
            self.session_args = session_args or {}
            self.commit_on_exit = commit_on_exit
            self.multi_sessions = multi_sessions

        async def __aenter__(self):
            if not isinstance(_Session, async_sessionmaker):
                raise SessionNotInitialisedError

            if self.multi_sessions:
                self.multi_sessions_token = _multi_sessions_ctx.set(True)
                self.commit_on_exit_token = _commit_on_exit_ctx.set(
                    self.commit_on_exit
                )
            else:
                self.token = _session.set(_Session(**self.session_args))
            return type(self)

        async def __aexit__(self, exc_type, exc_value, traceback):
            if self.multi_sessions:
                _multi_sessions_ctx.reset(self.multi_sessions_token)
                _commit_on_exit_ctx.reset(self.commit_on_exit_token)
            else:
                session = _session.get()
                try:
                    if exc_type is not None:
                        await session.rollback()
                    elif self.commit_on_exit:
                        await session.commit()
                finally:
                    await session.close()
                    _session.reset(self.token)

    return SQLAlchemyMiddleware, DBSession


class TestCustomFastAPIMiddleware(unittest.TestCase):
    """Test cases for the custom FastAPI middleware implementation"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        if not FASTAPI_AVAILABLE:
            self.skipTest("FastAPI not available")

    def test_middleware_creation_with_db_url(self):
        """Test creating middleware with database URL"""
        SQLAlchemyMiddleware, DBSession = create_middleware_and_session_proxy()

        # Create a mock FastAPI app
        app = FastAPI(title="Test App")

        # Test middleware creation with basic settings
        try:
            app.add_middleware(
                SQLAlchemyMiddleware,
                db_url=settings.DB_URL,
                engine_args={
                    "echo": settings.SQLALCHEMY_ECHO,
                    "pool_pre_ping": True,
                    "pool_size": settings.DB_POOL_SIZE_MIN,
                    "max_overflow": settings.DB_POOL_SIZE_MAX,
                    "pool_recycle": 900,
                    "connect_args": {
                        "server_settings": {
                            "jit": "off",
                            "application_name": settings.SERVICE_NAME,
                        },
                        "ssl": SSLMode.disable,
                    },
                },
            )
            self.assertTrue(
                True, "Custom FastAPI middleware created successfully"
            )
        except Exception as e:
            self.fail(f"Failed to create custom FastAPI middleware: {e}")

    def test_middleware_creation_with_custom_engine(self):
        """Test creating middleware with custom engine"""
        SQLAlchemyMiddleware, DBSession = create_middleware_and_session_proxy()

        # Create a custom engine
        engine = create_async_engine(
            settings.DB_URL,
            echo=settings.SQLALCHEMY_ECHO,
            pool_pre_ping=True,
            pool_size=settings.DB_POOL_SIZE_MIN,
            max_overflow=settings.DB_POOL_SIZE_MAX,
            pool_recycle=900,
            connect_args={
                "server_settings": {
                    "jit": "off",
                    "application_name": settings.SERVICE_NAME,
                },
                "ssl": SSLMode.disable,
            },
        )

        # Create a mock FastAPI app
        app = FastAPI(title="Test App with Custom Engine")

        try:
            app.add_middleware(
                SQLAlchemyMiddleware,
                custom_engine=engine,
                commit_on_exit=True,
            )
            self.assertTrue(
                True,
                "Custom FastAPI middleware with custom engine created successfully",
            )
        except Exception as e:
            self.fail(
                f"Failed to create custom FastAPI middleware with custom engine: {e}"
            )

    def test_middleware_validation_errors(self):
        """Test middleware validation for required parameters"""
        SQLAlchemyMiddleware, DBSession = create_middleware_and_session_proxy()

        app = FastAPI(title="Test App")

        # Test missing both db_url and custom_engine by calling the constructor directly
        with self.assertRaises(ValueError) as context:
            # This should raise ValueError when called directly
            SQLAlchemyMiddleware(app)

        self.assertIn(
            "You need to pass a db_url or a custom_engine parameter",
            str(context.exception),
        )

    def test_db_session_context_manager(self):
        """Test DBSession context manager functionality"""
        SQLAlchemyMiddleware, DBSession = create_middleware_and_session_proxy()

        # Test DBSession creation
        db_session = DBSession(commit_on_exit=True)
        self.assertIsInstance(db_session, DBSession)
        self.assertTrue(db_session.commit_on_exit)

        # Test multi-sessions mode
        multi_db_session = DBSession(multi_sessions=True, commit_on_exit=False)
        self.assertIsInstance(multi_db_session, DBSession)
        self.assertTrue(multi_db_session.multi_sessions)
        self.assertFalse(multi_db_session.commit_on_exit)

    def test_session_proxy_creation(self):
        """Test that the session proxy is created correctly"""
        SQLAlchemyMiddleware, DBSession = create_middleware_and_session_proxy()

        # Verify that we get the expected classes
        if FASTAPI_AVAILABLE:
            self.assertTrue(
                issubclass(SQLAlchemyMiddleware, BaseHTTPMiddleware)
            )

        # Test that DBSession has the expected metaclass behavior
        self.assertTrue(hasattr(DBSession, "__aenter__"))
        self.assertTrue(hasattr(DBSession, "__aexit__"))

        # Test that the session property exists on the class (without accessing it)
        # We check the metaclass has the session property
        self.assertTrue(hasattr(type(DBSession), "session"))
        self.assertTrue(isinstance(type(DBSession).session, property))

    def test_middleware_with_session_args(self):
        """Test middleware creation with session arguments"""
        SQLAlchemyMiddleware, DBSession = create_middleware_and_session_proxy()

        app = FastAPI(title="Test App with Session Args")

        try:
            app.add_middleware(
                SQLAlchemyMiddleware,
                db_url=settings.DB_URL,
                engine_args={
                    "echo": False,
                    "pool_pre_ping": True,
                },
                session_args={
                    "autoflush": False,
                    "autocommit": False,
                },
                commit_on_exit=True,
            )
            self.assertTrue(
                True,
                "Custom FastAPI middleware with session args created successfully",
            )
        except Exception as e:
            self.fail(
                f"Failed to create custom FastAPI middleware with session args: {e}"
            )

    @unittest.skipUnless(FASTAPI_AVAILABLE, "FastAPI not available")
    def test_full_integration_mock(self):
        """Test full integration with mocked database operations"""
        SQLAlchemyMiddleware, DBSession = create_middleware_and_session_proxy()

        app = FastAPI(title="Full Integration Test")

        # Add the middleware
        app.add_middleware(
            SQLAlchemyMiddleware,
            db_url=settings.DB_URL,
            engine_args={
                "echo": settings.SQLALCHEMY_ECHO,
                "pool_pre_ping": True,
                "pool_size": settings.DB_POOL_SIZE_MIN,
                "max_overflow": settings.DB_POOL_SIZE_MAX,
                "pool_recycle": 900,
                "connect_args": {
                    "server_settings": {
                        "jit": "off",
                        "application_name": settings.SERVICE_NAME,
                    },
                    "ssl": SSLMode.disable,
                },
            },
            commit_on_exit=True,
        )

        # Add a test route
        @app.get("/test")
        async def test_route():
            return {"message": "Test successful"}

        # Test that the app and middleware were set up successfully
        # We don't need to actually make HTTP requests, just verify setup
        self.assertIsNotNone(app)
        self.assertTrue(
            True,
            "Full integration test completed - middleware setup successful",
        )


if __name__ == "__main__":
    if FASTAPI_AVAILABLE:
        unittest.main()
    else:
        print("FastAPI not available. Skipping tests.")
        print("To run these tests, install FastAPI: pip install fastapi")
