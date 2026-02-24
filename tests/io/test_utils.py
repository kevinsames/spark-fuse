"""Tests for IO utility helpers."""

from __future__ import annotations

import pytest

from spark_fuse.io.utils import as_bool, as_seq


class TestAsSeq:
    def test_none_returns_none(self):
        assert as_seq(None) is None

    def test_csv_string_split(self):
        assert as_seq("a, b, c") == ["a", "b", "c"]

    def test_empty_string_returns_empty(self):
        assert as_seq("") == []

    def test_single_value_string(self):
        assert as_seq("foo") == ["foo"]

    def test_list_passthrough(self):
        assert as_seq(["x", "y"]) == ["x", "y"]

    def test_tuple_passthrough(self):
        assert as_seq(("x", "y")) == ["x", "y"]

    def test_non_iterable_stringified(self):
        assert as_seq(42) == ["42"]

    def test_whitespace_trimmed(self):
        assert as_seq("  a , b ") == ["a", "b"]

    def test_empty_segments_dropped(self):
        assert as_seq("a,,b,") == ["a", "b"]


class TestAsBool:
    def test_none_returns_default(self):
        assert as_bool(None, True) is True
        assert as_bool(None, False) is False

    def test_bool_passthrough(self):
        assert as_bool(True, False) is True
        assert as_bool(False, True) is False

    @pytest.mark.parametrize("value", ["1", "true", "True", "yes", "Y", "on"])
    def test_truthy_strings(self, value):
        assert as_bool(value, False) is True

    @pytest.mark.parametrize("value", ["0", "false", "False", "no", "N", "off"])
    def test_falsy_strings(self, value):
        assert as_bool(value, True) is False

    def test_non_string_truthy(self):
        assert as_bool(1, False) is True

    def test_non_string_falsy(self):
        assert as_bool(0, True) is False

    def test_unknown_string_uses_bool(self):
        assert as_bool("maybe", False) is True  # bool("maybe") is True
