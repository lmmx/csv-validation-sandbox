# From part 2 of https://github.com/dask/dask/issues/8045#issuecomment-902256015
from __future__ import annotations
import csv
import io
import re
import pandas as pd
from pytest import mark, raises

__all__ = ["reader", "validate_dictreader", "validate_str"]

sample_header_bytestr = b"intA,intB,strC\n"
sample_df = pd.read_csv(io.BytesIO(sample_header_bytestr))


def reader(input_str, df_colnames=sample_df.columns):
    """
    Args:
      input_str : The string to load into a new :class:`io.StringIO` buffer to create
                  the DictReader from
      df        : The DataFrame whose columns give the fieldnames fro the DictReader
    """
    buf = io.StringIO(input_str)
    return csv.DictReader(buf, fieldnames=df_colnames)


def validate_dictreader(
    r,
    escapechar=r"\\",
    quotechar='"',
    doublequote=True,
    printout=False,
) -> None | list[dict[str, str]]:
    """
    Args:
      r : (:class:`csv.DictReader`) The DictReader which can be iterated to give the rows to be validated
          in the form of :class:`dict` objects (one per row).
    """
    n_matchable = 1 + int(doublequote)
    re_patt_str = (
        rf".*((?<!({escapechar}))({quotechar}))" + r"{1," + f"{n_matchable}" + r"}.*"
    )
    re_patt = re.compile(re_patt_str)
    validated_rows = []
    output = []
    for row in r:
        if None in row.values():
            raise ValueError(f"Absent field (incomplete row) at {row=}")
        if any(re_patt.match(v) for v in row.values()):
            raise ValueError(f"{quotechar=} found in {row=}")
        validated_rows.append(row)
    for row in validated_rows:
        if printout:
            print(row)  # Only print rows if entire DataFrame validated
        else:
            output.append(row)
    return None if printout else output


def validate_str(input_str, sample_df=sample_df):
    """
    Validate the string ``input_str`` whose column names (or index if no names) are
    available through the ``sample_df`` DataFrame (sampled from the file's head).
    """
    r = reader(input_str, df_colnames=sample_df.columns)
    return validate_dictreader(r)


### Tests begin here


@mark.parametrize(
    "rows_str,expected",
    [
        (
            '1,2,hello\n3,4,world\n5,6,"foo\n7,8,bar\n9,10,baz"\n5,6,"foo\n',
            [
                {"intA": "1", "intB": "2", "strC": "hello"},
                {"intA": "3", "intB": "4", "strC": "world"},
                {"intA": "5", "intB": "6", "strC": "foo\n7,8,bar\n9,10,baz"},
                {"intA": "5", "intB": "6", "strC": "foo\n"},
            ],
        )
    ],
)
def test_multiline_rows_str(rows_str, expected):
    """
    Show the 1st multiline field value is parsed and the 2nd multiline field value is
    indicated as still 'open' at the end of the string by the presence of a newline at
    the end of the field value.
    """
    validated = validate_str(input_str=rows_str)
    print(validated)
    print(expected)
    assert validated == expected
    assert [*validated[-1].values()][-1].endswith("\n")


@mark.parametrize(
    "rows_str_rev,err_msg",
    [
        (
            '\n1,2,hello\n3,4,world\n5,6,"foo\n7,8,bar\n9,10,baz"\n5,6,"foo'[::-1],
            "quotechar='\"' found in row={'intA': 'oof\"', 'intB': '6', 'strC': '5'}",
        )
    ],
)
def test_multiline_rows_str_backwards(rows_str_rev, err_msg):
    """
    Show that the same string as before (but read right to left) with a 'completed'
    (closed) multiline field value and an 'open' (unclosed) multiline field value raises
    an error (thrown due to the newline at the end of that field value, since in a
    parsing situation this would correspond to a need to advance the reverse start
    position to the next [rightward, forward] lineterminator).
    """
    with raises(ValueError, match=err_msg):
        validate_str(input_str=rows_str_rev)


@mark.parametrize(
    "rows_str,err_msg",
    [
        (
            'hello,world,etc\nfoo"\netc,etc,etc\n',
            r"Absent field \(incomplete row\) at row={'intA': 'foo\"', 'intB': None, 'strC': None}",
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
