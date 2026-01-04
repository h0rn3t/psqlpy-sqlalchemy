"""Tests for connection edge cases and error handling to increase coverage."""

import sys
import uuid
from unittest.mock import patch

from psqlpy_sqlalchemy.connection import (
    _PARAM_PATTERN,
    _PY_VERSION,
    _UUID_PATTERN,
    _VALUES_PATTERN,
    _check_dml,
    _convert_uuid,
    _get_param_regex,
)


class TestUtilityFunctions:
    """Test utility functions for better coverage."""

    def test_convert_uuid_with_uuid_object(self):
        """Test UUID conversion with actual UUID object (pass-through)."""
        test_uuid = uuid.uuid4()
        result = _convert_uuid(test_uuid)
        # UUID objects are passed through unchanged
        assert result == test_uuid
        assert isinstance(result, uuid.UUID)

    def test_convert_uuid_with_uuid_string(self):
        """Test UUID conversion with UUID string (converts to UUID object)."""
        test_uuid = uuid.uuid4()
        test_str = str(test_uuid)
        result = _convert_uuid(test_str)
        # UUID strings are converted to UUID objects for psqlpy binary protocol
        assert result == test_uuid
        assert isinstance(result, uuid.UUID)

    def test_convert_uuid_with_non_uuid(self):
        """Test UUID conversion with non-UUID string."""
        test_value = "not-a-uuid"
        result = _convert_uuid(test_value)
        # Non-UUID strings are passed through unchanged
        assert result == test_value

    def test_check_dml_insert(self):
        """Test DML detection for INSERT."""
        is_dml, upper_query = _check_dml("INSERT INTO table VALUES (1)")
        assert is_dml is True
        assert "INSERT" in upper_query

    def test_check_dml_update(self):
        """Test DML detection for UPDATE."""
        is_dml, upper_query = _check_dml("UPDATE table SET col=1")
        assert is_dml is True
        assert "UPDATE" in upper_query

    def test_check_dml_delete(self):
        """Test DML detection for DELETE."""
        is_dml, upper_query = _check_dml("DELETE FROM table")
        assert is_dml is True
        assert "DELETE" in upper_query

    def test_check_dml_select(self):
        """Test DML detection for SELECT (should be False)."""
        is_dml, upper_query = _check_dml("SELECT * FROM table")
        assert is_dml is False
        assert "SELECT" in upper_query

    def test_check_dml_with_returning(self):
        """Test DML detection with RETURNING clause."""
        is_dml, upper_query = _check_dml(
            "INSERT INTO table VALUES (1) RETURNING id"
        )
        assert is_dml is False  # RETURNING makes it not DML for batching
        assert "RETURNING" in upper_query

    def test_get_param_regex_caching(self):
        """Test parameter regex caching."""
        pattern1 = _get_param_regex("test_param")
        pattern2 = _get_param_regex("test_param")
        assert pattern1 is pattern2  # Should be cached

    def test_regex_patterns(self):
        """Test compiled regex patterns."""
        # Test parameter pattern
        match = _PARAM_PATTERN.search(":param_name")
        assert match is not None
        assert match.group(1) == "param_name"

        # Test UUID pattern
        test_uuid = str(uuid.uuid4())
        assert _UUID_PATTERN.match(test_uuid) is not None
        assert _UUID_PATTERN.match("invalid-uuid") is None

        # Test VALUES pattern
        assert (
            _VALUES_PATTERN.search("INSERT INTO t VALUES (1, 2)") is not None
        )


class TestPythonVersionOptimizations:
    """Test Python version-specific optimizations."""

    def test_python_version_detection(self):
        """Test Python version detection."""
        assert sys.version_info[:2] == _PY_VERSION

    @patch("psqlpy_sqlalchemy.connection._PY_VERSION", (3, 10))
    def test_legacy_uuid_conversion(self):
        """Test UUID conversion is now version-independent."""
        # UUID conversion is now the same for all Python versions
        from psqlpy_sqlalchemy.connection import _convert_uuid

        test_uuid = uuid.uuid4()
        result = _convert_uuid(test_uuid)
        # UUID objects are passed through unchanged
        assert result == test_uuid
        assert isinstance(result, uuid.UUID)

    @patch("psqlpy_sqlalchemy.connection._PY_VERSION", (3, 10))
    def test_legacy_dml_check(self):
        """Test DML check for older Python versions."""
        from psqlpy_sqlalchemy.connection import _check_dml

        is_dml, upper_query = _check_dml("INSERT INTO table VALUES (1)")
        assert is_dml is True
        assert "INSERT" in upper_query


class TestPerformanceOptimizations:
    """Test performance-related optimizations."""

    def test_regex_pattern_compilation(self):
        """Test that regex patterns are pre-compiled."""
        # All patterns should be compiled regex objects

        from psqlpy_sqlalchemy.connection import (
            _PARAM_PATTERN,
            _UUID_PATTERN,
            _VALUES_PATTERN,
        )

        assert hasattr(_PARAM_PATTERN, "pattern")
        assert hasattr(_UUID_PATTERN, "pattern")
        assert hasattr(_VALUES_PATTERN, "pattern")

    def test_frozenset_optimizations(self):
        """Test frozenset optimizations for keyword lookups."""
        from psqlpy_sqlalchemy.connection import _DML_KEYWORDS

        # Should be a frozenset for O(1) lookup
        assert isinstance(_DML_KEYWORDS, frozenset)
        assert "INSERT" in _DML_KEYWORDS
        assert "UPDATE" in _DML_KEYWORDS
        assert "DELETE" in _DML_KEYWORDS

    def test_lru_cache_usage(self):
        """Test LRU cache usage for parameter regex."""
        from psqlpy_sqlalchemy.connection import _get_param_regex

        # Should be decorated with lru_cache
        assert hasattr(_get_param_regex, "cache_info")

        # Test cache behavior
        pattern1 = _get_param_regex("test")
        pattern2 = _get_param_regex("test")
        assert pattern1 is pattern2  # Should be cached

        cache_info = _get_param_regex.cache_info()
        assert cache_info.hits > 0

    def test_python_version_optimizations(self):
        """Test Python version-specific optimizations."""
        from psqlpy_sqlalchemy.connection import (
            _PY_VERSION,
            _check_dml,
            _convert_uuid,
        )

        # Test that version-specific functions are defined
        assert _PY_VERSION is not None
        assert callable(_convert_uuid)
        assert callable(_check_dml)

        # UUID conversion now converts strings to UUID objects
        test_uuid = uuid.uuid4()
        result = _convert_uuid(test_uuid)
        assert result == test_uuid
        assert isinstance(result, uuid.UUID)
