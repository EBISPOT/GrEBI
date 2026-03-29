# `grebi_ingest_sssom`

Ingests mapping data in SSSOM (Simple Standard for Sharing Ontological Mappings) TSV format. Reads an SSSOM file from stdin and writes GrEBI edge JSONL to stdout.

SSSOM files have a YAML header embedded in `#`-prefixed comment lines, followed by a TSV body. The YAML header's `curie_map` is used to expand CURIEs into full IRIs during ingestion.

## Usage

```
cat mappings.sssom.tsv | grebi_ingest_sssom > output.jsonl
```

## Options

None. This tool takes no arguments.

## Behaviour

1. Lines starting with `#` are parsed as a YAML header. The `curie_map` section is extracted and used to build a prefix map for expanding CURIEs.
2. The remaining lines are parsed as TSV with headers.
3. The `subject_id`, `predicate_id`, and `object_id` columns are used to construct edges.
4. All CURIEs (in subject, predicate, object, and metadata columns) are expanded using the prefix map from the header.
5. Each row is emitted as a GrEBI edge record where:
   - `id` is the subject
   - The predicate is the edge type key
   - The object is the `grebi:value`
   - All other columns are included in `grebi:properties`
