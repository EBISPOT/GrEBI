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
These tests exercise the complete functionality of the program with realistic data and validate outputs:
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
- **grebi_rdf2jsonl** - Tests RDF to JSONL conversion
- **grebi_parquet2jsonl** - Tests Parquet to JSONL conversion (requires PyArrow)
- **grebi_ingest_gwas** - Tests GWAS catalog data ingestion
- **grebi_ingest_kgx_edges** - Tests KGX edge data ingestion
- **grebi_ingest_ols** - Tests OLS ontology data ingestion
- **grebi_ingest_reactome** - Tests Reactome pathway data ingestion
- **grebi_ingest_sqlite** - Tests SQLite database ingestion
- **grebi_index** - Tests metadata and search index building
- **grebi_link** - Tests edge creation from property values
- **grebi_make_neo_csv** - Tests Neo4j CSV file generation
- **grebi_make_neo_ids_csv** - Tests Neo4j ID CSV generation
- **grebi_make_solr** - Tests Solr index data creation
- **grebi_link_results** - Tests query result linking with metadata
- **grebi_make_compressed_blob** - Tests compressed blob creation
- **grebi_make_sqlite** - Tests SQLite database creation

### Test with Existing Infrastructure
- **grebi_merge** - Has existing test in tests/test.sh

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
