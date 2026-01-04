"""Tests for advanced connection features and optimizations."""

import uuid
from unittest.mock import patch

import pytest

from psqlpy_sqlalchemy.connection import (
    _DML_KEYWORDS,
    _PARAM_PATTERN,
    _PY_VERSION,
    _UUID_PATTERN,
    _VALUES_PATTERN,
    _check_dml,
    _convert_uuid,
    _get_param_regex,
)


class TestPerformanceOptimizations:
    """Test performance-related optimizations."""

    def test_regex_pattern_compilation(self):
        """Test that regex patterns are pre-compiled."""
        # All patterns should be compiled regex objects

        assert hasattr(_PARAM_PATTERN, "pattern")
        assert hasattr(_UUID_PATTERN, "pattern")
        assert hasattr(_VALUES_PATTERN, "pattern")

    def test_frozenset_optimizations(self):
        """Test frozenset optimizations for keyword lookups."""
        # Should be a frozenset for O(1) lookup
        assert isinstance(_DML_KEYWORDS, frozenset)
        assert "INSERT" in _DML_KEYWORDS
        assert "UPDATE" in _DML_KEYWORDS
        assert "DELETE" in _DML_KEYWORDS

    def test_lru_cache_usage(self):
        """Test LRU cache usage for parameter regex."""
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
        # Test that version-specific functions are defined
        assert _PY_VERSION is not None
        assert callable(_convert_uuid)
        assert callable(_check_dml)

        # Test UUID conversion - now converts strings to UUID objects
        test_uuid = uuid.uuid4()
        # UUID objects are passed through
        result = _convert_uuid(test_uuid)
        assert result == test_uuid
        assert isinstance(result, uuid.UUID)

        # UUID strings are converted to UUID objects
        test_uuid_str = str(test_uuid)
        result_str = _convert_uuid(test_uuid_str)
        assert result_str == test_uuid
        assert isinstance(result_str, uuid.UUID)


class TestUtilityFunctionEdgeCases:
    """Test utility function edge cases."""

    def test_convert_uuid_with_none(self):
        """Test UUID conversion with None."""
        result = _convert_uuid(None)
        assert result is None

    def test_convert_uuid_with_non_uuid_string(self):
        """Test UUID conversion with non-UUID string."""
        test_string = "not-a-uuid"
        result = _convert_uuid(test_string)
        # Non-UUID strings are passed through unchanged
        assert result == test_string

    def test_convert_uuid_with_uuid_string(self):
        """Test UUID conversion with valid UUID string."""
        test_uuid = uuid.uuid4()
        test_string = str(test_uuid)
        result = _convert_uuid(test_string)
        # Valid UUID strings are converted to UUID objects
        assert result == test_uuid
        assert isinstance(result, uuid.UUID)

    def test_check_dml_with_whitespace(self):
        """Test DML detection with leading whitespace."""
        is_dml, upper_query = _check_dml("   INSERT INTO table VALUES (1)")
        assert is_dml is True
        assert "INSERT" in upper_query

    def test_check_dml_with_lowercase(self):
        """Test DML detection with lowercase."""
        is_dml, upper_query = _check_dml("insert into table values (1)")
        assert is_dml is True
        assert "INSERT" in upper_query

    def test_check_dml_with_mixed_case_returning(self):
        """Test DML detection with mixed case RETURNING."""
        is_dml, upper_query = _check_dml(
            "INSERT INTO table VALUES (1) returning id"
        )
        assert is_dml is False  # RETURNING makes it not DML for batching
        assert "RETURNING" in upper_query

    def test_check_dml_with_create_statement(self):
        """Test DML detection with CREATE statement."""
        is_dml, upper_query = _check_dml("CREATE TABLE test (id INT)")
        assert is_dml is False
        assert "CREATE" in upper_query

    def test_get_param_regex_with_special_chars(self):
        """Test parameter regex with special characters."""
        pattern = _get_param_regex("test_param_123")
        assert pattern is not None

        # Test that it matches the parameter
        match = pattern.search(":test_param_123")
        assert match is not None

    def test_get_param_regex_cache_different_params(self):
        """Test parameter regex cache with different parameters."""
        pattern1 = _get_param_regex("param1")
        pattern2 = _get_param_regex("param2")
        pattern3 = _get_param_regex("param1")  # Should be cached

        assert pattern1 is not pattern2
        assert pattern1 is pattern3


class TestRegexPatternMatching:
    """Test regex pattern matching edge cases."""

    def test_param_pattern_with_type_cast(self):
        """Test parameter pattern with type casting."""
        match = _PARAM_PATTERN.search(":param_name::UUID")
        assert match is not None
        assert match.group(1) == "param_name"
        assert match.group(2) == "::UUID"

    def test_param_pattern_without_type_cast(self):
        """Test parameter pattern without type casting."""
        match = _PARAM_PATTERN.search(":param_name")
        assert match is not None
        assert match.group(1) == "param_name"
        assert match.group(2) is None

    def test_param_pattern_with_underscore(self):
        """Test parameter pattern with underscore."""
        match = _PARAM_PATTERN.search(":user_id")
        assert match is not None
        assert match.group(1) == "user_id"

    def test_param_pattern_with_numbers(self):
        """Test parameter pattern with numbers."""
        match = _PARAM_PATTERN.search(":param123")
        assert match is not None
        assert match.group(1) == "param123"

    def test_uuid_pattern_valid_uuids(self):
        """Test UUID pattern with valid UUIDs."""
        valid_uuids = [
            "550e8400-e29b-41d4-a716-446655440000",
            "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
            "6ba7b811-9dad-11d1-80b4-00c04fd430c8",
        ]

        for uuid_str in valid_uuids:
            assert _UUID_PATTERN.match(uuid_str) is not None
            assert _UUID_PATTERN.match(uuid_str.upper()) is not None

    def test_uuid_pattern_invalid_uuids(self):
        """Test UUID pattern with invalid UUIDs."""
        invalid_uuids = [
            "not-a-uuid",
            "550e8400-e29b-41d4-a716",  # Too short
            "550e8400-e29b-41d4-a716-446655440000-extra",  # Too long
            "550e8400-e29b-41d4-a716-44665544000g",  # Invalid character
            "",
            "550e8400e29b41d4a716446655440000",  # No dashes
        ]

        for uuid_str in invalid_uuids:
            assert _UUID_PATTERN.match(uuid_str) is None

    def test_values_pattern_matching(self):
        """Test VALUES pattern matching."""
        queries_with_values = [
            "INSERT INTO table VALUES (1, 2, 3)",
            "INSERT INTO table VALUES (1, 'test')",
            "insert into table values (1, 2)",
            "INSERT INTO table VALUES ($1, $2)",
        ]

        for query in queries_with_values:
            assert _VALUES_PATTERN.search(query) is not None

    def test_values_pattern_non_matching(self):
        """Test VALUES pattern with non-matching queries."""
        queries_without_values = [
            "SELECT * FROM table",
            "UPDATE table SET col = 1",
            "DELETE FROM table",
            "CREATE TABLE test (id INT)",
        ]

        for query in queries_without_values:
            assert _VALUES_PATTERN.search(query) is None


class TestPythonVersionCompatibility:
    """Test Python version compatibility features."""

    def test_python_version_tuple(self):
        """Test Python version tuple format."""
        assert isinstance(_PY_VERSION, tuple)
        assert len(_PY_VERSION) == 2
        assert all(isinstance(x, int) for x in _PY_VERSION)

    @patch("psqlpy_sqlalchemy.connection._PY_VERSION", (3, 9))
    def test_legacy_python_version_handling(self):
        """Test handling for legacy Python versions."""
        # Import functions that might have version-specific implementations
        from psqlpy_sqlalchemy.connection import _check_dml, _convert_uuid

        # UUID conversion is now version-independent
        test_uuid = uuid.uuid4()
        result = _convert_uuid(test_uuid)
        assert result == test_uuid
        assert isinstance(result, uuid.UUID)

        is_dml, upper_query = _check_dml("INSERT INTO table VALUES (1)")
        assert is_dml is True

    def test_current_python_version_optimizations(self):
        """Test optimizations for current Python version."""
        # Test that optimizations are applied based on current version
        if _PY_VERSION >= (3, 11):
            # UUID conversion is now the same for all versions
            test_uuid = uuid.uuid4()
            result = _convert_uuid(test_uuid)
            assert result == test_uuid
            assert isinstance(result, uuid.UUID)

        if _PY_VERSION >= (3, 12):
            # Test string optimization path
            is_dml, upper_query = _check_dml("INSERT INTO table VALUES (1)")
            assert is_dml is True


class TestConstantDefinitions:
    """Test constant definitions and their properties."""

    def test_dml_keywords_completeness(self):
        """Test that all DML keywords are included."""
        expected_keywords = {"INSERT", "UPDATE", "DELETE"}
        assert expected_keywords.issubset(_DML_KEYWORDS)

    def test_dml_keywords_immutability(self):
        """Test that DML keywords frozenset is immutable."""
        with pytest.raises(AttributeError):
            _DML_KEYWORDS.add("SELECT")  # Should raise AttributeError

    def test_regex_pattern_properties(self):
        """Test regex pattern properties."""
        # Test that patterns are compiled and have expected properties
        assert _PARAM_PATTERN.pattern is not None
        assert _UUID_PATTERN.pattern is not None
        assert _VALUES_PATTERN.pattern is not None

        # Test flags
        assert _UUID_PATTERN.flags & 2  # re.IGNORECASE flag
        assert _VALUES_PATTERN.flags & 2  # re.IGNORECASE flag


class TestCachePerformance:
    """Test cache performance and behavior."""

    def test_param_regex_cache_size(self):
        """Test parameter regex cache size limit."""
        # Clear cache first
        _get_param_regex.cache_clear()

        # Generate many different parameter names
        for i in range(300):  # More than cache size (256)
            _get_param_regex(f"param_{i}")

        cache_info = _get_param_regex.cache_info()
        assert cache_info.maxsize == 256
        assert cache_info.currsize <= 256

    def test_param_regex_cache_hits(self):
        """Test parameter regex cache hit ratio."""
        _get_param_regex.cache_clear()

        # Access same parameter multiple times
        param_name = "test_param"
        for _ in range(10):
            _get_param_regex(param_name)

        cache_info = _get_param_regex.cache_info()
        assert cache_info.hits >= 9  # Should have 9 hits after first miss
        assert cache_info.misses >= 1  # Should have at least 1 miss

    def test_param_regex_cache_clear(self):
        """Test parameter regex cache clearing."""
        # Add some entries
        _get_param_regex("test1")
        _get_param_regex("test2")

        # Clear cache
        _get_param_regex.cache_clear()

        cache_info = _get_param_regex.cache_info()
        assert cache_info.currsize == 0
        assert cache_info.hits == 0
        assert cache_info.misses == 0
