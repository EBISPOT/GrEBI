# `grebi_ingest_kgx_edges`

Ingests edge data in KGX (Knowledge Graph Exchange) JSONL format. Reads KGX edge records from stdin and converts them into GrEBI edge JSONL on stdout.

Each KGX edge has `subject`, `predicate`, and `object` fields. These are mapped to a GrEBI edge where the subject becomes the `id`, the predicate becomes the edge type key, and the object becomes the `grebi:value`. All other fields are carried as edge metadata in `grebi:properties`.

## Usage

```
cat kgx_edges.jsonl | grebi_ingest_kgx_edges [OPTIONS] > output.jsonl
```

## Options

All options are optional.

| Option | Description |
|---|---|
| `--kgx_rename_field <FROM:TO>` | Rename a metadata field. Can be specified multiple times. |
| `--kgx_inject_key_prefix <PREFIX>` | Prefix all metadata keys (that don't already contain `:`) with this string. Defaults to empty. |

## Behaviour

1. Each input line is parsed as a JSON object with `subject`, `predicate`, and `object` string fields.
2. Lines with missing or non-string subject/predicate/object are skipped with a warning.
3. Remaining fields are included as edge properties, with keys optionally renamed or prefixed.
4. Null values are dropped.
