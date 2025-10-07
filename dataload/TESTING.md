# GrEBI Rust Programs Test Suite

This directory contains integration tests for all 26 Rust dataload programs in the GrEBI pipeline.

## Test Structure

Each program has a `tests/` directory containing:
- `test.sh` - Test script that exercises the program
- Test input files (e.g., `input.jsonl`, `input.tsv`, `groups.txt`) - Sample data for testing
- Any other necessary test fixtures

## Running Tests

### Run all tests
```bash
cd dataload
./run_all_tests.sh
```

### Run a specific test
```bash
cd dataload/<program_directory>/tests
./test.sh
```

## Test Types

### Full Integration Tests
These tests exercise the complete functionality of the program with realistic data:
- **grebi_unwind** - Tests unwinding arrays in JSONL
- **grebi_nodes2edges** - Tests converting node properties to edges
- **grebi_tsv2jsonl** - Tests TSV to JSONL conversion
- **grebi_transform_jsonl** - Tests JSON transformation operations
- **grebi_extract_identifiers** - Tests identifier extraction
- **grebi_identifiers2groups** - Tests grouping equivalent identifiers
- **grebi_assign_ids** - Tests canonical ID assignment
- **grebi_superclasses2types** - Tests type inference from superclasses
- **grebi_normalise_prefixes** - Tests prefix normalization
- **grebi_ingest_sssom** - Tests SSSOM mapping ingestion

### Smoke Tests
These tests verify that programs exist and basic functionality works:
- **grebi_merge** - Has existing test in tests/test.sh
- **grebi_index** - Verifies program runs and shows help
- **grebi_link** - Verifies program exists
- **grebi_make_neo_csv** - Verifies program exists
- **grebi_make_neo_ids_csv** - Verifies program exists
- **grebi_ingest_ols** - Verifies program exists
- **grebi_ingest_gwas** - Verifies program exists
- **grebi_ingest_kgx_edges** - Verifies program exists
- **grebi_ingest_reactome** - Verifies program exists
- **grebi_ingest_sqlite** - Verifies program exists
- **grebi_rdf2jsonl** - Verifies program exists
- **grebi_parquet2jsonl** - Verifies program exists
- **grebi_make_solr** - Verifies program exists
- **grebi_link_results** - Verifies program exists
- **grebi_make_compressed_blob** - Verifies program exists
- **grebi_make_sqlite** - Verifies program exists

## Continuous Integration

Tests are automatically run on push and pull requests via GitHub Actions. See `.github/workflows/test-rust.yml`.

## Test Guidelines

When adding new tests:
1. Create a `tests/` directory in the program's directory
2. Add a `test.sh` script that tests the program's core functionality
3. Include minimal test input files
4. Validate outputs where possible (check line counts, JSON validity, expected values)
5. For complex programs, smoke tests are acceptable
6. Make the script executable: `chmod +x test.sh`

## Test Input Files

Test input files (*.jsonl, *.tsv, *.txt) are tracked in git despite being in `.gitignore` by using `git add -f`.
This ensures test data is available for CI/CD pipelines.

## Dependencies

Tests require:
- Rust 1.90.0+
- jq (for JSON validation)
- Standard Unix tools (grep, wc, etc.)

## Program Descriptions

### 01_ingest - Data Ingestion
- **grebi_rdf2jsonl** - Converts RDF to JSONL
- **grebi_tsv2jsonl** - Converts TSV to JSONL with array support
- **grebi_parquet2jsonl** - Converts Parquet to JSONL
- **grebi_nodes2edges** - Extracts edges from node properties
- **grebi_ingest_sssom** - Ingests SSSOM mapping files
- **grebi_ingest_ols** - Ingests OLS ontology data
- **grebi_ingest_gwas** - Ingests GWAS catalog data
- **grebi_transform_jsonl** - Transforms JSONL (select/remove/rename keys, inject types/prefixes)
- **grebi_ingest_reactome** - Ingests Reactome pathway data
- **grebi_ingest_kgx_edges** - Ingests KGX edge data
- **grebi_ingest_sqlite** - Ingests SQLite database data
- **grebi_normalise_prefixes** - Normalizes identifier prefixes using prefix maps
- **grebi_unwind** - Unwinds array fields into multiple records

### 02_assign_ids - ID Assignment
- **grebi_extract_identifiers** - Extracts identifiers from JSONL objects
- **grebi_identifiers2groups** - Groups equivalent identifiers into cliques
- **grebi_assign_ids** - Assigns canonical IDs based on groups
- **grebi_superclasses2types** - Infers types from superclass relationships

### 03_merge - Data Merging
- **grebi_merge** - Merges multiple datasource files by ID

### 04_index - Indexing
- **grebi_index** - Builds metadata and search indexes

### 05_link - Linking
- **grebi_link** - Creates edges from property values referencing other entities

### 06_create_neo_db - Neo4j Database Creation
- **grebi_make_neo_csv** - Generates Neo4j CSV files
- **grebi_make_neo_ids_csv** - Generates Neo4j ID mapping CSVs

### 08_create_other_dbs - Other Database Exports
- **grebi_make_solr** - Creates Solr index data
- **grebi_link_results** - Links query results with metadata
- **grebi_make_compressed_blob** - Creates compressed blobs for SQLite
- **grebi_make_sqlite** - Creates SQLite database
