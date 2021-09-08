# From part 2 of https://github.com/dask/dask/issues/8045#issuecomment-902256015
from __future__ import annotations
import io
import re
import pandas as pd
from pandas.errors import ParserError
from pytest import mark, raises

__all__ = ["reader", "validate_df", "validate_str"]

sample_header_bytestr = b"intA,intB,strC\n"
sample_df = pd.read_csv(io.BytesIO(sample_header_bytestr))


def validate_df(
    df,
    escapechar=r"\\",
    quotechar='"',
    doublequote=True,
    printout=False,
) -> None | list[dict[str, str]]:
    """
    Args:
      df : (:class:`pd.DataFrame`) The DataFrame which can be iterated to give the
           rows to be validated in the form of :class:`pd.Series` objects (one per row).
    """
    n_matchable = 1 + int(doublequote)
    # Regex to match an unescaped quotechar
    re_patt_str = (
        rf".*((?<!({escapechar}))({quotechar}))" + r"{1," + f"{n_matchable}" + r"}.*"
    )
    re_patt = re.compile(re_patt_str)
    validated_rows = []
    output = []
    for row_idx, row_series in df.iterrows():
        row = row_series.to_dict()
        if float("nan") in row.values():
            raise ValueError(f"Absent field (incomplete row) at {row=}")
        if any(isinstance(v, str) and re_patt.match(v) for v in row.values()):
            raise ValueError(f"{quotechar=} found in {row=}")
        validated_rows.append(row)
    if printout:
        print()
    for row in validated_rows:
        if printout:
            print(row)  # Only print rows if entire DataFrame validated
        else:
            output.append(row)
    return None if printout else output


def validate_str(input_str, sample_colnames=sample_df.columns):
    """
    Validate the string ``input_str`` whose column names (or index if no names) are
    available through the ``sample_df`` DataFrame (sampled from the file's head).
    """
    df = pd.read_csv(io.StringIO(input_str), names=sample_colnames)
    return validate_df(df, printout=False)


### Tests begin here


@mark.parametrize(
    "rows_str,expected",
    [
        (
            '1,2,hello\n3,4,world\n5,6,"foo\n7,8,bar\n9,10,baz"\n',
            [
                {"intA": 1, "intB": 2, "strC": "hello"},
                {"intA": 3, "intB": 4, "strC": "world"},
                {"intA": 5, "intB": 6, "strC": "foo\n7,8,bar\n9,10,baz"},
            ],
        )
    ],
)
def test_multiline_rows_str_closed(rows_str, expected):
    """
    Show the 'closed' multiline field value is parsed.
    """
    validated = validate_str(input_str=rows_str)
    print(f"{validated=}")
    print(f"{expected=}")
    assert validated == expected


@mark.parametrize(
    "rows_str,err_msg",
    [
        (
            '1,2,hello\n3,4,world\n5,6,"foo\n7,8,bar\n9,10,baz"\n5,6,"foo\n',
            "Error tokenizing data. C error: EOF inside string starting at row 3",
            # [
            #    {"intA": "1", "intB": "2", "strC": "hello"},
            #    {"intA": "3", "intB": "4", "strC": "world"},
            #    {"intA": "5", "intB": "6", "strC": "foo\n7,8,bar\n9,10,baz"},
            #    {"intA": "5", "intB": "6", "strC": "foo\n"},
            # ],
        )
    ],
)
def test_multiline_rows_str_open(rows_str, err_msg):
    """
    Show the 1st multiline field value is parsed but the 2nd multiline field value is
    still 'open' at the end of the string and causes a :class:`pd.errors.ParserError`.
    """
    with raises(ParserError, match=err_msg):
        df = pd.read_csv(io.StringIO(rows_str), names=sample_df.columns)
        # validated = validate_str(input_str=rows_str) # Don't need the full routine


@mark.parametrize(
    "rows_str_rev,err_msg",
    [
        (
            '\n1,2,hello\n3,4,world\n5,6,"foo\n7,8,bar\n9,10,baz"\n5,6,"foo'[::-1],
            "quotechar='\"' found in row={'strC': 'oof\"', 'intB': 6, 'intA': 5}",
        )
    ],
)
def test_multiline_rows_str_backwards(rows_str_rev, err_msg):
    """
    Show that the same string as before (but read right to left) with a 'completed'
    (closed) multiline field value and an 'open' (unclosed) multiline field value does
    not raise a :class:`pd.errors.ParserError` but can be caught 'manually'.

    The 'manual' ValueError is thrown due to the quotechar at the end of that field
    value [which, a little confusingly, is actually at the start of the field value
    in the original left-to-right direction].

    In a parsing situation this would correspond to a need to advance the reverse start
    position to the next [rightward, forward] lineterminator.

    Note that this is identical to the way a non-row-terminating lineterminator is
    handled for the csv module (in `validate_rows_test.py`).
    """
    with raises(ValueError, match=err_msg):
        validate_str(input_str=rows_str_rev, sample_colnames=sample_df.columns[::-1])


@mark.parametrize(
   "rows_str,err_msg",
   [
       (
           'hello,world,etc\nfoo"\netc,etc,etc\n',
           #"Absent field \(incomplete row\) at row={'intA': 'foo\"', 'intB': nan, 'strC': nan}",
           "quotechar.* found in.*", # Actually precedes the absent field error...
       )
   ],
)
def test_absent_field_str(rows_str, err_msg):
   """
   The newline comes directly after the quotechar, indicating that where a new row
   was expected to start, it turned out that the previous row was invalid and in fact
   a single multi-line field within the row that then closed at the substring "foo".
   At this point, an error is raised.
   """
   with raises(ValueError, match=err_msg):
       validate_str(input_str=rows_str)
