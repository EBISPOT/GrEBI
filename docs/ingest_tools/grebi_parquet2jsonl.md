# `grebi_parquet2jsonl`

Converts Parquet data into JSONL. Reads the entire Parquet file from stdin into memory and writes line-delimited JSON to stdout using the Arrow JSON writer.

## Usage

```
cat input.parquet | grebi_parquet2jsonl > output.jsonl
```

## Options

None. This tool takes no arguments.
