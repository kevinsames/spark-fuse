"""Tests for shared IO HTTP utilities."""

from __future__ import annotations

from spark_fuse.io._http import normalize_jsonable, validate_http_url


class TestNormalizeJsonable:
    def test_primitive_passthrough(self):
        assert normalize_jsonable("hello") == "hello"
        assert normalize_jsonable(42) == 42
        assert normalize_jsonable(3.14) == 3.14
        assert normalize_jsonable(True) is True
        assert normalize_jsonable(None) is None

    def test_dict_keys_stringified(self):
        assert normalize_jsonable({1: "a", 2: "b"}) == {"1": "a", "2": "b"}

    def test_nested_mapping(self):
        result = normalize_jsonable({"outer": {"inner": [1, 2]}})
        assert result == {"outer": {"inner": [1, 2]}}

    def test_list_and_tuple(self):
        assert normalize_jsonable([1, "a", None]) == [1, "a", None]
        assert normalize_jsonable((1, 2)) == [1, 2]

    def test_set_converted_to_list(self):
        result = normalize_jsonable({1})
        assert isinstance(result, list)
        assert result == [1]

    def test_unknown_type_stringified(self):
        class Custom:
            def __str__(self):
                return "custom-repr"

        assert normalize_jsonable(Custom()) == "custom-repr"

    def test_deeply_nested(self):
        data = {"a": [{"b": (1, 2)}, "c"]}
        result = normalize_jsonable(data)
        assert result == {"a": [{"b": [1, 2]}, "c"]}


class TestValidateHttpUrl:
    def test_valid_http(self):
        assert validate_http_url("http://example.com") is True

    def test_valid_https(self):
        assert validate_http_url("https://example.com/api") is True

    def test_ftp_rejected(self):
        assert validate_http_url("ftp://example.com") is False

    def test_empty_string(self):
        assert validate_http_url("") is False

    def test_non_string(self):
        assert validate_http_url(42) is False  # type: ignore[arg-type]

    def test_none(self):
        assert validate_http_url(None) is False  # type: ignore[arg-type]
