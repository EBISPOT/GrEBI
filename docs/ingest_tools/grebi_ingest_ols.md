# `grebi_ingest_ols`

Ingests ontology data from the OLS (Ontology Lookup Service) JSON export format. Reads a JSON document containing ontologies with their classes, properties, and individuals from stdin and writes GrEBI JSONL to stdout.

## Usage

```
cat ols_export.json | grebi_ingest_ols --ontologies <IDS> [OPTIONS] > output.jsonl
```

## Options

| Option | Required | Description |
|---|---|---|
| `--ontologies <IDS>` | **Yes** | Comma-separated list of ontology IDs to include (e.g. `efo,mondo,hp`). Ontologies not in this list are skipped. |
| `--defining_only` | No | Flag. If set, only include entities that are defined by the ontology being processed (skip imported terms). |
| `--skip_obsolete` | No | Flag. If set, skip entities marked as obsolete. |

## Environment Variables

| Variable | Description |
|---|---|
| `GREBI_DATASOURCE_ID` | If set, used as the `id` for the ontology metadata record instead of the ontology ID from the file. |

## Behaviour

1. The input JSON is expected to have a top-level `ontologies` array.
2. Each ontology object contains metadata fields and arrays of `classes`, `properties`, and `individuals`.
3. An ontology metadata record is emitted with `grebi:type` set to `["ols:Ontology", "grebi:Datasource"]`.
4. Each class, property, and individual is emitted as a separate JSONL record with OLS-prefixed property names.
5. Ontologies not listed in `--ontologies` are skipped entirely.
