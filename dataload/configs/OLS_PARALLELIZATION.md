# OLS Ingest Parallelization

## Overview
This change parallelizes OLS ontology ingestion by creating individual config files for each ontology, allowing them to be processed independently and in parallel by workflow systems like Nextflow.

## Changes Made

### 1. Individual OLS Config Files
- Created `generate_ols_configs.py` script to automatically generate individual YAML config files
- Generated 30 individual config files (one per ontology):
  - `ols_efo.yaml`, `ols_mp.yaml`, `ols_hp.yaml`, `ols_go.yaml`, `ols_ro.yaml`, etc.
- Each config uses `grebi_ingest_ols` with a single ontology ID via the `--ontologies` parameter

### 2. Updated Subgraph Configs
Updated the following Python config files to reference individual OLS configs instead of combined ones:
- `ebi_monarch.py` - Now references 30 individual OLS configs
- `hett.py` - Now references 9 individual OLS configs
- `impc.py` - Now references 30 individual OLS configs
- `impc_x_gwas.py` - Now references 22 individual OLS configs
- `gwas_and_efo.py` - Now references 1 individual OLS config (efo)

### 3. Regenerated JSON Configs
- Regenerated all subgraph JSON configs using the Makefile
- Verified all configs are valid YAML/JSON

## How It Works

### Before
```yaml
name: OLS
ingests:
  - globs: ["dataload/00_fetch_data/ols/ontologies_linked.json.gz"]
    command: 'grebi_ingest_ols --ontologies efo,mp,hp,go,...,ecto'
```
Single process reading the entire JSON file, processing all ontologies sequentially.

### After
```yaml
# ols_efo.yaml
name: OLS.efo
ingests:
  - globs: ["dataload/00_fetch_data/ols/ontologies_linked.json.gz"]
    command: 'grebi_ingest_ols --ontologies efo'

# ols_mp.yaml
name: OLS.mp
ingests:
  - globs: ["dataload/00_fetch_data/ols/ontologies_linked.json.gz"]
    command: 'grebi_ingest_ols --ontologies mp'
```
Multiple processes can read the same input file in parallel, each processing only their designated ontology.

## Benefits

1. **Parallelization**: Each ontology can be processed independently and in parallel
2. **Faster Processing**: Multiple ontologies can be ingested simultaneously
3. **Better Resource Utilization**: Nextflow can distribute work across available resources
4. **Easier Debugging**: Individual ontology failures are isolated
5. **Incremental Processing**: Can re-run individual ontologies without reprocessing all

## Implementation Details

### Existing Code Support
The `grebi_ingest_ols` tool already supports filtering by ontology:
- It accepts a comma-separated list of ontology IDs via `--ontologies`
- Uses a whitelist to filter ontologies from the input JSON
- Skips ontologies not in the whitelist efficiently

### Input File Handling
- All configs still use the same input file (`dataload/00_fetch_data/ols/ontologies_linked.json.gz`)
- Multiple processes can safely read the same compressed JSON file in parallel
- The tool uses streaming JSON parsing, so memory usage remains constant per process

### Nextflow Integration
Nextflow's existing workflow will automatically:
- Process each config file as a separate task
- Schedule tasks in parallel based on available resources
- Handle failures independently per ontology

## Validation

All generated configs have been validated:
- ✓ 30 OLS YAML config files are syntactically valid
- ✓ All subgraph JSON configs are valid
- ✓ Datasource counts are correct in each subgraph

## Regenerating Configs

To regenerate the individual OLS configs:
```bash
cd dataload/configs/datasource_configs
python3 generate_ols_configs.py
```

To regenerate subgraph JSON configs:
```bash
cd dataload/configs/subgraph_configs
make
```
