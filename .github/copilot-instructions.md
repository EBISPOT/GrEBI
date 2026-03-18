# GrEBI (Graphs@EBI) - GitHub Copilot Instructions

## Project Overview

GrEBI is an HPC pipeline that aggregates knowledge graphs from EMBL-EBI resources, MONARCH Initiative KG, ROBOKOP, Ubergraph, and other biomedical sources. The project enables integrative queries spanning multiple biomedical resources.

## Repository Structure

- **dataload/**: Rust-based HPC pipeline for data processing
  - Data ingestion, ID assignment, merging, indexing, linking
  - Neo4j database creation
  - Solr and SQLite exports
- **webapp/**: Web application stack
  - `grebi_api/`: Java Spring Boot REST API
  - `grebi_ui/`: TypeScript/React frontend
  - `grebi_metadata_service/`: Java Spring Boot metadata service
  - `grebi_resolver_service/`: Java Spring Boot resolver service
- **query_templates/**: YAML query templates for common queries
- **materialised_queries/**: Cypher queries that are periodically executed
- **configs/**: YAML configuration files for datasources and subgraphs

## Technology Stack

### Rust (dataload pipeline)
- **Version**: 1.90.0
- **Build**: `cargo build --release`
- **Style**: Follow standard Rust conventions (rustfmt)
- **Dependencies**: Managed via Cargo.toml
- **Key Libraries**: 
  - serde_json for JSON processing
  - clap for CLI argument parsing
  - Custom grebi_shared library for common functionality

### Java (Spring Boot services)
- **Version**: Java 17+
- **Build**: Maven (`mvn clean install`)
- **Framework**: Spring Boot
- **Style**: Follow standard Java conventions
- **Package Structure**: `uk.ac.ebi.grebi.*`

### TypeScript/React (UI)
- **Framework**: React with Material-UI components
- **Build**: npm/yarn and esbuild
- **Style**: Use functional components with hooks
- **State Management**: React hooks (useState, useMemo, etc.)
- **Syntax Highlighting**: Uses PrismJS for code display

### Python (configuration scripts)
- **Version**: Python 3.11
- **Style**: PEP 8
- **Usage**: Primarily for generating YAML configurations

## Code Style Guidelines

### General
- Follow the existing code style in each language
- Use descriptive variable names
- Add comments for complex logic, but prefer self-documenting code
- Keep functions small and focused

### Rust
- Use `snake_case` for functions and variables
- Use `PascalCase` for types and traits
- Prefer `Result` and `Option` over panicking
- Use `eprintln!` for progress/debug messages to stderr
- Stream processing: Read from stdin, write to stdout when possible

### Java
- Use `camelCase` for methods and variables
- Use `PascalCase` for classes
- Follow Spring Boot conventions for controllers, services, and repositories
- Use `@Autowired` for dependency injection
- Use SLF4J for logging

### TypeScript/React
- Use `camelCase` for variables and functions
- Use `PascalCase` for components
- Prefer functional components with hooks
- Use TypeScript types/interfaces for props
- Use Material-UI components consistently

## Data Processing Patterns

### Pipeline Architecture
1. **download.nf**: Nextflow pipeline that downloads raw data from external sources to `$GREBI_DOWNLOADS_PATH`
2. **01_ingest/**: Transform data into JSONL format and extract identifiers
3. **02_assign_ids/**: Build equivalence groups and assign canonical IDs to nodes
4. **03_merge/**: Merge properties about the same clique from multiple files into one JSON object per clique
5. **04_index/**: Build metadata and search indexes
6. **05_link/**: Create edges from property values that reference other entities
7. **06_create_neo_db/**: Generate Neo4j CSV files and create Neo4j database
8. **07_run_queries/**: Execute materialised queries on the Neo4j database
9. **08_create_other_dbs/**: Export to Solr, SQLite, and other formats

### Data Format
- Internal representation: JSONL (newline-delimited JSON)
- Identifiers: Canonical CURIE format via Bioregistry
- Graph nodes: Each node represents a clique of equivalent entities and is linked to multiple source IDs
- Graph edges: Represented as relationships between cliques

### Integration Strategy
- Canonicalize all identifiers to CURIE format using the Bioregistry
- Property values that are identifiers become edges
- Merge equivalent nodes via clique analysis
- Use qualified safe labels for property names

## Docker and Containerization

- Custom Docker images: `ghcr.io/ebispot/grebi_*`
- Neo4j version: 2025.03.0-community
- Solr version: 9.8.1
- Python version: 3.11
- Rust version: 1.90.0

## Query Templates

- Stored in YAML files under `query_templates/`
- Include Cypher queries, Python code, and metadata
- Topics defined in `_topics.yaml`
- Loaded via `GrebiQueryTemplatesRepo` in the API

## Testing and Building

### Rust
```bash
cargo test
cargo build --release
```

### Java
```bash
mvn clean test
mvn clean install
```

### TypeScript/React
```bash
npm test
npm run build
```

## Common Tasks

### Adding a New Datasource
1. Create YAML config in `configs/datasource_configs/` with `download:` entries (dest + sources) and `ingests:` entries
2. Add ingestion logic in `dataload/01_ingest/` if needed
3. Update subgraph config to include the new datasource

### Adding a New Query Template
1. Create YAML file in `query_templates/`
2. Include cypher query, description, and parameters
3. Update `_topics.yaml` if adding a new topic
4. Restart API service to load new templates

### Modifying the UI
1. Components are in `webapp/grebi_ui/src/components/`
2. Use Material-UI components for consistency
3. Test locally with `npm start`
4. Build for production with `npm run build`

## Biomedical Domain Knowledge

- **Ontologies**: OLS, PHENIO, Uberon, Gene Ontology
- **Identifiers**: CURIE format normalised using the Bioregistry with lowercase prefixes (e.g., `hgnc:123`, `hp:0001234`)
- **Mappings**: SSSOM format, skos:exactMatch, semapv:crossSpeciesExactMatch
- **Knowledge Graphs**: Uses Biolink Model concepts
- **Phenotypes**: Human Phenotype Ontology (HP), Mammalian Phenotype Ontology (MP)
- **Genes**: HGNC (human), MGI (mouse), ZFIN (zebrafish)
- **Diseases**: MONDO, EFO, MedGen
- **Drugs**: ChEMBL, DrugCentral
- **Pathways**: Reactome, Gene Ontology

## Performance Considerations

- Pipeline designed for HPC environments (Slurm)
- Neo4j graphs can be very large (100M+ nodes, 800M+ edges)
- Memory requirements: 32GB+ RAM for querying
- Use streaming processing where possible
- Prefer buffered I/O for large files
- Consider pagination for API responses

## Important Notes

- Neo4j version must be 2025.03.0-community for compatibility
- Solr version must be 9.8.1 for compatibility
- CURIE format is standardized via Bioregistry
- Cross-species phenotype matching uses special mapping predicates
- Nodes in GrEBI represent cliques of equivalent entities from multiple sources
