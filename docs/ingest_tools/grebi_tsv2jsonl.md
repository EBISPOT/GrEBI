# `grebi_tsv2jsonl`

Converts TSV (tab-separated values) data into JSONL. Reads from stdin and writes to stdout. Automatically skips leading comment lines starting with `#`.

## Usage

```
cat input.tsv | grebi_tsv2jsonl [OPTIONS] > output.jsonl
```

## Options

All options are optional.

| Option | Description |
|---|---|
| `--tsv_columns <COLUMNS>` | Comma-separated list of column names to use instead of the header row. If provided, the first line of input is treated as data. |
| `--tsv_array_delimiter <DELIM>` | If set, values in each field are split by this delimiter and emitted as JSON arrays. Without this, each field is emitted as a single-element array. |
| `--tsv_ignore_empty_fields` | Flag. Skip fields with empty string values. |

## Behaviour

1. Lines starting with `#` at the beginning of the file are skipped (e.g. CTD header comments).
2. The first non-comment line is treated as a header row (unless `--tsv_columns` is provided).
3. Each subsequent row is emitted as a JSON object where keys are column names and values are arrays of strings.
4. If `--tsv_array_delimiter` is set, field values are split on that delimiter into multi-element arrays.
