# GrEBI (Graphs@EBI)

HPC pipeline using ontologies and LLM embeddings to aggregate knowledge graphs from [EMBL-EBI resources](https://www.ebi.ac.uk/services/data-resources-and-tools), the [MONARCH Initiative](https://monarch-initiative.github.io/monarch-ingest/Sources/), [DisMech](https://dismech.monarchinitiative.org/), [ROBOKOP](https://robokop.renci.org/), [Ubergraph](https://github.com/INCATools/ubergraph), and other sources.

The aim is to make it easier for humans and machines to perform integrative queries which span multiple biomedical resources, in contrast to existing REST APIs which are typically constrainted to one resource.

A development server with the output of this pipeline can be accessed at https://wwwdev.ebi.ac.uk/kg

MCP endpoint: https://wwwdev.ebi.ac.uk/kg/api/v1/mcp (Streamable HTTP)

The GrEBI pipeline is being applied to a number of projects including the [International Mouse Phenotyping Consortium (IMPC)](https://www.mousephenotype.org/) knowledge graph and the [EMBL Human Ecosystems Transversal Theme (HETT)](https://www.embl.org/about/info/human-ecosystems/) ExposomeKG.

<img src="https://www.embl.org/guidelines/design/wp-content/uploads/2022/02/EMBL_logo_colour_DIGITAL.png" width=100 />&nbsp;&nbsp;&nbsp;&nbsp;<img src="https://monarch-initiative.github.io/monarch-ingest/images/monarch-initiative.png" width=100 />&nbsp;&nbsp;&nbsp;&nbsp;<img src="https://www.mousephenotype.org/wp-content/uploads/2022/08/IMPC_logo.svg" width=100 />

## Making the tests pass

GrEBI has a suite of automated E2E tests that run the full pipeline on small synthetic datasets and compare the resulting Neo4j/Postgres database contents against committed expected output in `tests/expected_output/`. If code changes alter the pipeline output such that it no longer matches the expected snapshots, the CI will fail and you will need to update the expected output.

There are nine test subgraphs, each exercising a different aspect of the pipeline:

| Test subgraph | Purpose |
| --- | --- |
| `test_clique_merge` | Verifies equivalent entities are merged into a single clique |
| `test_edge_linking` | Verifies property values referencing other entities become graph edges |
| `test_multi_datasource` | Verifies merging data from two separate datasources |
| `test_type_hierarchy` | Verifies type superclass propagation through `rdfs:subClassOf` |
| `test_ubergraph` | Builds a tiny ubergraph with owlmake and ingests the redundant and non-redundant closures as separate datasources |
| `test_pdbe` | Ingests a small real subset of PDBe SIFTS mappings: row merging, edge linking and ontology mapping |
| `test_gwas` | Ingests a few real GWAS Catalog rows covering the packed `MAPPED_GENE` / `MAPPED_TRAIT` column formats |
| `test_reactome` | Ingests a few real Reactome dump objects and checks a DOID-annotated disease clique-merges into the ontology term |
| `test_primekg` | Ingests a few real PrimeKG rows and checks their unpadded MONDO/HPO/GO/UBERON ids land on the ontology terms |

The UI also has unit tests (Vitest and Testing Library) that run in a few seconds without any data:

    cd webapp/grebi_ui && npm ci && npm test

So does the API (JUnit and Mockito): the routes and the MCP server run over mocked repositories with the query templates in `webapp/grebi_api/src/test/resources/query_templates`, and `mvn verify` also applies the coverage gate:

    cd webapp/grebi_api && mvn verify

And so do the Rust dataload crates. The shared library has unit tests, and every pipeline binary has golden tests: the built binary runs on recorded inputs (mostly lifted from the E2E test subgraphs) and its output must match the recorded output byte for byte. The cases live in each crate's `tests/golden/`. When a change to a binary is intended, rerun with `UPDATE_GOLDEN=1` to rewrite the recorded outputs, then review the diff before committing:

    cd dataload && cargo test --workspace
    cd dataload && UPDATE_GOLDEN=1 cargo test -p grebi_merge

### Prerequisites

You need Docker with the `docker compose` plugin and enough disk space to build the image. Build it locally before running the tests:

    docker build -t ghcr.io/ebispot/grebi_combined:dev .

### Running all tests

Run the full E2E test suite across all four test subgraphs:

    bash tests/run_all_e2e.sh

This will run each test subgraph through the full Nextflow pipeline (ingest → assign IDs → merge → index → link → create Neo4j → run queries → create Postgres → integration tests), export DB snapshots, and compare them against `tests/expected_output/`.

### Running a single test

To run only one test subgraph:

    bash tests/run_e2e.sh test_clique_merge

### Updating expected output

When your changes intentionally alter the pipeline output, you need to update the expected snapshots. Run the pipeline for the affected test subgraph, inspect the changes, and commit them:

    export GREBI_SUBGRAPHS=test_clique_merge
    export GREBI_NF_EXTRA_ARGS="--export_snapshots true"
    bash dataload/scripts/dataload_local.sh

Copy the new snapshots to expected output:

    cp out/test_clique_merge/test_clique_merge_snapshot_*.jsonl \
       tests/expected_output/test_clique_merge/

Now inspect the changes with `git diff` and make sure they are intentional. When you are happy, stage and commit the updated expected output:

    git add -A tests/expected_output/
    git commit -m "Update expected test output"

## Database links on node pages

Source ids on node pages, in search results and in property values link out to the database they come from, with the database's icon, and datasource tags link to the datasource's homepage. The data behind that lives in the UI, so changing it never needs a data load:

- `webapp/grebi_ui/src/db_links/db_links.json` holds a URL pattern per Bioregistry prefix, with EBI-hosted identifiers routed to their EBI home (OLS for ontology terms), regex rules for ids that carry no prefix such as GWAS Catalog accessions, and the datasource homepages.
- `webapp/grebi_ui/dist/db_icons/` holds one icon per database.

Both are generated. Edit the curated tables in the generator rather than the outputs, then regenerate (the `--check` flag fetches every curated URL with an example id) and rebuild the UI:

    cd webapp/grebi_ui && uv run scripts/make_db_links.py --check
