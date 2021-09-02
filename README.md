# CSV Validation Sandbox

Sandbox to work out and demonstrate a workflow for validation of CSVs which may have multiline
fields, to be contributed to
[fsspec-reference-maker](https://github.com/intake/fsspec-reference-maker/issues/66) and
ultimately to [dask](https://github.com/dask/dask/issues/8045).

This repo contains tests (which can be run with `pytest tests/*`):

- `validate_rows.py`, as for the original, but covering these test cases formally in pytest
  - `deprecated/original_validate_rows.py`, the original file used to outline the validation
    edge cases and expected behaviour
- `lineterm_support.py`, a pytest suite demonstrating that the lineterminator argument to
  `csv.DictReader` does nothing while the argument to pandas works as expected.
