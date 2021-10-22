from pathlib import Path
import time
from fsspec_reference_maker.csv import SingleCsvToPartitions
from pytest import mark

# p = Path.home() / "dev/wikitransp/src/wikitransp/data/store/wit_v1.train.all-1percent_sample.tsv"
dummy_path = Path.home() / "dev/testing/wikitransp/my_dummy_file.tsv"
multiline_dummy_text = """it\ts
me\tlo
uis\t:)
hel\t"lo
wor\tld"
aga\tin...!
"""
#  it            s
#  0   me           lo
#  1  uis           :)
#  2  hel  lo\nwor\tld
#  3  aga       in...!

# Newline at pos 24 becomes 25 and is no longer row-terminating,
# therefore whichever would start at 26 will instead start at 34

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
            multiline_dummy_text,
            {
                0: ["{{u}}", 0, 11],    # 0
                11: ["{{u}}", 11, 15],  # 10
                26: ["{{u}}", 26, 8],   # 20
                34: ["{{u}}", 34, 11],  # 30
                ## 40: ["{{u}}", 45, 0], # 40 : Skipped (EOF)
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
            multiline_dummy_text,
            {
                0: ['{{u}}', 0, 5],
                5: ['{{u}}', 5, 6],
                11: ['{{u}}', 11, 7],
                18: ['{{u}}', 18, 8],
                26: ['{{u}}', 26, 8],
                34: ['{{u}}', 34, 11]
                ## 35: ["{{u}}", 45, 0], # 35 : Skipped (blocksize overshoot)
                ## 40: ["{{u}}", 45, 0], # 40 : Skipped (blocksize overshoot)
                ## 45: ["{{u}}", 45, 0], # 45 : Skipped (EOF)
            },
        )
    ],
)
def test_row_over_blocksize(file_text, expected):
    """
    Use a file with row length smaller than the blocksize. It has 0-length offsets.
    """
    partitions = evaluate_partitions(file_text, n_columns=2, blocksize=5)
    assert partitions.store == expected


@mark.parametrize(
    "file_text,expected",
    [
        (
            multiline_dummy_text,
            {
                0: ["{{u}}", 0, 26],     # 0
                26: ["{{u}}", 26, 19],  # 20
                ## 40: ["{{u}}", 44, 0], # 40 : Skipped (EOF)
            },
        )
    ],
)
def test_row_under_blocksize(file_text, expected):
    """
    Use a file with row length smaller than the blocksize.
    """
    partitions = evaluate_partitions(file_text, n_columns=2, blocksize=20)
    assert partitions.store == expected
