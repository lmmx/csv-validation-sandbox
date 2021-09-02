# CSV Validation Sandbox

Sandbox to work out and demonstrate a workflow for validation of CSVs which may have multiline
fields, to be contributed to
[fsspec-reference-maker](https://github.com/intake/fsspec-reference-maker/issues/66) and
ultimately to [dask](https://github.com/dask/dask/issues/8045).

This repo contains:

- `original_validate_rows.py`, the original file used to outline the validation edge cases and
  expected behaviour
- `validate_rows.py`, as for the original, but covering these test cases formally in pytest
