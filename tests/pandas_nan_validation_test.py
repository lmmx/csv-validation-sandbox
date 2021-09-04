# From https://github.com/lmmx/csv-validation-sandbox/issues/1#issuecomment-912498007
from __future__ import annotations
import io
import pandas as pd
from pytest import mark

__all__ = []


def trivial_return(value):
    "Return a value unchanged (used to override pandas CSV parser dtype conversion)."
    return value


def make_df(n_cols: int, rows_str: str) -> pd.DataFrame:
    """
    Read a CSV into a DataFrame without NaN value conversion so that any None values are
    only present due to a missing field, permitting a check for parsed CSV column count.
    """
    return pd.read_csv(
        io.StringIO(rows_str),
        keep_default_na=False,
        na_filter=False,
        na_values=[],
        engine="python",
        converters=dict.fromkeys(range(n_cols), trivial_return),
    )


@mark.parametrize("n_cols", [2])
@mark.parametrize(
    "rows_str,expected",
    [
        (
            "hello,world\nfoo\nbar,baz\n",
            {"hello": {0: "foo", 1: "bar"}, "world": {0: None, 1: "baz"}},
        ),
        (
            "hello,world\nfoo\n3,4\n",
            {"hello": {0: "foo", 1: "3"}, "world": {0: None, 1: "4"}},
        ),
    ],
)
def test_absent_field_str(n_cols, rows_str, expected):
    """
    Show that an absent field can be detected from the pandas parser as ``None`` if the
    converters are passed as a trivial function returning its input (preventing dtype conversion).
    """
    df = make_df(n_cols, rows_str)
    assert df.to_dict() == expected


@mark.parametrize("n_cols", [2])
@mark.parametrize(
    "rows_str,expected",
    [
        (
            "hello,world\nfoo,\nbar,baz\n",
            {"hello": {0: "foo", 1: "bar"}, "world": {0: "", 1: "baz"}},
        ),
        (
            "hello,world\nfoo,\n3,4\n",
            {"hello": {0: "foo", 1: "3"}, "world": {0: "", 1: "4"}},
        ),
    ],
)
def test_empty_field_str(n_cols, rows_str, expected):
    """
    Show that an empty field can be detected from the pandas parser as the empty string if the
    converters are passed as a trivial function returning its input (preventing dtype conversion).
    """
    df = make_df(n_cols, rows_str)
    assert df.to_dict() == expected
