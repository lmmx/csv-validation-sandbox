import csv
import io
import pandas as pd
from pytest import mark


@mark.parametrize(
    "sample_str,sep,linesep,expected",
    [
        (
            "a b c~1 2 3~4 5 6~7 8 9",
            " ",
            "~",
            [],
        )
    ],
)
def test_no_lineterm_support(sample_str, sep, linesep, expected):
    """
    Show that the csv module can't read a CSV with non-standard lineterminator (a tilde)
    because the enumerated rows of the reader is an empty list, i.e. nothing is read.
    """
    r = csv.DictReader(io.StringIO(sample_str), delimiter=sep, lineterminator=linesep)
    assert list(r) == expected


@mark.parametrize(
    "sample_str,sep,expected",
    [
        (
            "a b c\n1 2 3\n4 5 6\n7 8 9",
            " ",
            [
                {"a": "1", "b": "2", "c": "3"},
                {"a": "4", "b": "5", "c": "6"},
                {"a": "7", "b": "8", "c": "9"},
            ],
        )
    ],
)
def test_implicit_lineterm_support(sample_str, sep, expected):
    """
    Show that the csv module can read a CSV with the newline char `\n` without it being
    specified (implicitly "\n" or "\r" are used).
    """
    r = csv.DictReader(io.StringIO(sample_str), delimiter=sep)
    assert list(r) == expected


@mark.parametrize(
    "sample_str,sep,linesep,expected",
    [
        (
            "a b c~1 2 3~4 5 6~7 8 9",
            " ",
            "~",
            list(range(1, 10)),
        )
    ],
)
def test_implicit_lineterm_support(sample_str, sep, linesep, expected):
    """
    Show that pandas can read a CSV with the tilde char.
    """
    df = pd.read_csv(io.StringIO(sample_str), sep=sep, lineterminator=linesep)
    assert df.values.ravel().tolist() == expected
