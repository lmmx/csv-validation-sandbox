# From https://github.com/lmmx/csv-validation-sandbox/issues/1#issuecomment-912498007
from __future__ import annotations
import io
import pandas as pd
from pytest import mark, raises
import re

__all__ = ["validate_df", "validate_str", "trivial_return", "make_df"]

def validate_df(
    df,
    escapechar=r"\\",
    quotechar='"',
    doublequote=True,
    verbose=True,
) -> None | list[dict[str, str]]:
    """
    Validate that the DataFrame does not contain ``None`` values and does not have
    ``quotechar`` characters within any field values. Arguments other than those below
    are the same as those passed to :function:`pandas.read_csv` (``escapechar``,
    ``quotechar``, ``doublequote``).

    Args:
      df      : (:class:`pd.DataFrame`) DataFrame which can be iterated to give rows to
                be validated in the form of :class:`pd.Series` objects (one per row).
      verbose : Whether to print the rows once validated (default: ``False``)
    """
    n_matchable = 1 + int(doublequote)
    re_patt_str = (
        rf".*((?<!({escapechar}))({quotechar}))" + r"{1," + f"{n_matchable}" + r"}.*"
    )
    re_patt = re.compile(re_patt_str)
    validated_rows = []
    output = []
    for row_idx, row_series in df.iterrows():
        row = row_series.to_dict()
        if None in row.values():
            raise ValueError(f"Absent field (incomplete row) at {row=}")
        if any(isinstance(v, str) and re_patt.match(v) for v in row.values()):
            raise ValueError(f"{quotechar=} found in {row=}")
        validated_rows.append(row)
    if verbose:
        for row in validated_rows:
            print(row)  # Only print rows if entire DataFrame validated
    return validated_rows


def validate_str(input_str, sample_colnames=None):
    """
    Validate the string ``input_str``, using the provided column names.
    """
    df = make_df(input_str, names=sample_colnames)
    return validate_df(df)

def trivial_return(value):
    "Return a value unchanged (used to override pandas CSV parser dtype conversion)."
    return value


def make_df(n_cols: int, rows_str: str, names: list[str] | None = None) -> pd.DataFrame:
    """
    Read a CSV into a DataFrame without NaN value conversion so that any None values are
    only present due to a missing field, permitting a check for parsed CSV column count.
    """
    return pd.read_csv(
        io.StringIO(rows_str),
        names=names,
        keep_default_na=False,
        na_filter=False,
        na_values=[],
        engine="python",
        converters=dict.fromkeys(range(n_cols), trivial_return),
    )


@mark.parametrize("n_cols", [2])
@mark.parametrize(
    "rows_str,expected,err_msg",
    [
        (
            "hello,world\nfoo\nbar,baz\n",
            {"hello": {0: "foo", 1: "bar"}, "world": {0: None, 1: "baz"}},
            "Absent field \(incomplete row\) at row={'hello': 'foo', 'world': None}",
        ),
        (
            "hello,world\nfoo\n3,4\n",
            {"hello": {0: "foo", 1: "3"}, "world": {0: None, 1: "4"}},
            "Absent field \(incomplete row\) at row={'hello': 'foo', 'world': None}",
        ),
    ],
)
def test_absent_field_str(n_cols, rows_str, expected, err_msg):
    """
    Show that an absent field can be detected from the pandas parser as ``None`` if the
    converters are passed as a trivial function returning its input (preventing dtype conversion).
    """
    df = make_df(n_cols, rows_str)
    assert df.to_dict() == expected
    with raises(ValueError, match=err_msg):
        validate_df(df)


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
    validate_df(df)
