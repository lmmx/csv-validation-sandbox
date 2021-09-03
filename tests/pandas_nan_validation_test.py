# From https://github.com/lmmx/csv-validation-sandbox/issues/1#issuecomment-912498007
import io
import pandas as pd
from pytest import mark

__all__ = []

def trivial_return(value):
    "Return the value from the CSV parser completely unchanged."
    return value

@mark.parametrize(
        "rows_str,n_cols,expected",
        [
            (
                "hello,world\nfoo\nbar,baz\n",
                2,
                {"hello": {0: "foo", 1: "bar"}, "world": {0: None, 1: "baz"}},
            )
        ],
)
def test_absent_field_str(rows_str, n_cols, expected):
    """
    Show that an absent field can be detected from the pandas parser as ``None`` if the
    converters are passed as the string class (preventing dtype conversion).
    """
    df = pd.read_csv(
        io.StringIO(rows_str), keep_default_na=False, na_filter=False, na_values=[],
        engine="python", converters=dict.fromkeys(range(n_cols), trivial_return),
    )
    assert df.to_dict() == expected
