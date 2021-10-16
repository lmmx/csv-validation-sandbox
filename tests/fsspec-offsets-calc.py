from pathlib import Path
import time
from fsspec_reference_maker.csv import SingleCsvToPartitions
from pytest import mark

# p = Path.home() / "dev/wikitransp/src/wikitransp/data/store/wit_v1.train.all-1percent_sample.tsv"
dummy_path = Path.home() / "dev/testing/wikitransp/my_dummy_file.tsv"
simple_dummy_text = """it\ts
me\tlo
uis\t:)
hel\tlo
wor\tld
aga\tin...!
"""


def evaluate_partitions(file_text, **partition_kwargs):
    """
    Write text to a file and calculate the partitions with provided
    """
    file_path = dummy_path
    file_path.write_text(file_text)
    url = str(file_path)
    with open(file_path) as csv:
        partitions = SingleCsvToPartitions(csv, url, **partition_kwargs)
        partitions.translate()
    return partitions


@mark.parametrize(
    "file_text,expected",
    [
        (
            simple_dummy_text,
            {
                0: ["{{u}}", 0, 11],    # 0
                11: ["{{u}}", 11, 14],  # 10
                25: ["{{u}}", 25, 7],   # 20
                32: ["{{u}}", 32, 11],  # 30
                # 40: ["{{u}}", 43, 0], # 40
            },
        )
    ],
)
def test_row_even_blocksize(file_text, expected):
    """
    Use a file with row length roughly equal to the blocksize.
    """
    partitions = evaluate_partitions(file_text, n_columns=2, blocksize=10)
    assert partitions.store == expected


@mark.parametrize(
    "file_text,expected",
    [
        (
            simple_dummy_text,
            {
                0: ["{{u}}", 0, 5],     # 0
                5: ["{{u}}", 5, 6],     # 5
                11: ["{{u}}", 11, 7],   # 10
                18: ["{{u}}", 18, 7],   # 15
                # 20: ["{{u}}", 25, 0], # 20
                25: ["{{u}}", 25, 7],   # 25
                32: ["{{u}}", 32, 11],  # 30
                # 35: ["{{u}}", 43, 0], # 35
                # 40: ["{{u}}", 43, 0], # 40
            },
        )
    ],
)
def test_row_under_blocksize(file_text, expected):
    """
    Use a file with row length smaller than the blocksize. It has 0-length offsets.
    """
    partitions = evaluate_partitions(file_text, n_columns=2, blocksize=5)
    assert partitions.store == expected


@mark.parametrize(
    "file_text,expected",
    [
        (
            simple_dummy_text,
            {
                0: ["{{u}}", 0, 25],    # 0
                25: ["{{u}}", 25, 18],  # 20
                # 40: ["{{u}}", 43, 0], # 40
            },
        )
    ],
)
def test_row_over_blocksize(file_text, expected):
    """
    Use a file with row length smaller than the blocksize.
    """
    partitions = evaluate_partitions(file_text, n_columns=2, blocksize=20)
    assert partitions.store == expected
