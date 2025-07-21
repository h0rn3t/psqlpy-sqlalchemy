from .dialect import PSQLPyAsyncDialect

# Backward compatibility alias
PsqlpyDialect = PSQLPyAsyncDialect

__version__ = "0.1.0"
__all__ = ["PsqlpyDialect", "PSQLPyAsyncDialect"]
