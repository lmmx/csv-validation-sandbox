# From part 2 of https://github.com/dask/dask/issues/8045#issuecomment-902256015
import csv
import io
import re
import pandas as pd

__all__ = ["reader", "validate_dictreader", "validate_str"]

sample_header_bytestr = b'intA,intB,strC\n'
sample_df = pd.read_csv(io.BytesIO(sample_header_bytestr))

def reader(input_str, df=sample_df):
    """
    Args:
      input_str : The string to load into a new :class:`io.StringIO` buffer to create
                  the DictReader from
      df        : The DataFrame whose columns give the fieldnames fro the DictReader
    """
    buf = io.StringIO(input_str)
    return csv.DictReader(buf, fieldnames=df.columns)

def validate_dictreader(r):
    """
    Args:
      r : (:class:`csv.DictReader`) The DictReader which can be iterated to give the rows to be validated
          in the form of :class:`dict` objects (one per row).
    """
    escapechar = r"\\"
    quotechar = '"'
    doublequote = True
    n_matchable = 1 + int(doublequote)
    re_patt_str = rf'.*((?<!({escapechar}))({quotechar}))' + r'{1,' + f'{n_matchable}' + r'}.*'
    re_patt = re.compile(re_patt_str)
    validated_rows = []
    for row in r:
        if None in row.values():
            raise ValueError(f"Absent field (incomplete row) at {row=}")
        if any(re_patt.match(v) for v in row.values()):
            raise ValueError(f"{quotechar=} found in {row=}")
        validated_rows.append(row)
    for row in validated_rows:
        print(row) # Only print rows if entire DataFrame validated

def validate_str(input_str, sample_df=sample_df):
    """
    Validate the string ``input_str`` whose column names (or index if no names) are
    available through the ``sample_df`` DataFrame (sampled from the file's head).
    """
    r = reader(input_str, df=sample_df)
    validate_dictreader(r)
