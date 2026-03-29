
# Ingest tools

The GrEBI dataload pipeline includes a set of Rust command-line tools for ingesting, converting, and transforming data into the internal JSONL format. These tools are designed to be composed via Unix pipes.

## Format converters

These tools convert external data formats into JSONL:

- [`grebi_tsv2jsonl`](ingest_tools/grebi_tsv2jsonl.md) — TSV to JSONL
- [`grebi_rdf2jsonl`](ingest_tools/grebi_rdf2jsonl.md) — RDF (XML, Turtle, N-Quads) to JSONL
- [`grebi_parquet2jsonl`](ingest_tools/grebi_parquet2jsonl.md) — Parquet to JSONL

## Domain-specific ingesters

- [`grebi_ingest_ols`](ingest_tools/grebi_ingest_ols.md) — OLS ontology JSON exports
- [`grebi_ingest_kgx_edges`](ingest_tools/grebi_ingest_kgx_edges.md) — KGX edge JSONL
- [`grebi_ingest_sssom`](ingest_tools/grebi_ingest_sssom.md) — SSSOM mapping TSV files

## Transformation utilities

These transform data that is already represented in JSONL:

- [`grebi_transform_jsonl`](ingest_tools/grebi_transform_jsonl.md) — General-purpose JSONL transformer
- [`grebi_nodes2edges`](ingest_tools/grebi_nodes2edges.md) — Extract edges from node properties
- [`grebi_unwind`](ingest_tools/grebi_unwind.md) — Unwind array fields into separate records

