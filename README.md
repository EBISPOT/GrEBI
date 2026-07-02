# GrEBI (Graphs@EBI)

HPC pipeline using ontologies and LLM embeddings to aggregate knowledge graphs from [EMBL-EBI resources](https://www.ebi.ac.uk/services/data-resources-and-tools), the [MONARCH Initiative](https://monarch-initiative.github.io/monarch-ingest/Sources/), [DisMech](https://dismech.monarchinitiative.org/), [ROBOKOP](https://robokop.renci.org/), [Ubergraph](https://github.com/INCATools/ubergraph), and other sources.

The aim is to make it easier for humans and machines to perform integrative queries which span multiple biomedical resources, in contrast to existing REST APIs which are typically constrainted to one resource.

A development server with the output of this pipeline can be accessed at https://wwwdev.ebi.ac.uk/kg

MCP endpoint: https://wwwdev.ebi.ac.uk/kg/api/v1/mcp (Streamable HTTP)

The GrEBI pipeline is being applied to a number of projects including the [International Mouse Phenotyping Consortium (IMPC)](https://www.mousephenotype.org/) knowledge graph and the [EMBL Human Ecosystems Transversal Theme (HETT)](https://www.embl.org/about/info/human-ecosystems/) ExposomeKG.

<img src="https://www.embl.org/guidelines/design/wp-content/uploads/2022/02/EMBL_logo_colour_DIGITAL.png" width=100 />&nbsp;&nbsp;&nbsp;&nbsp;<img src="https://monarch-initiative.github.io/monarch-ingest/images/monarch-initiative.png" width=100 />&nbsp;&nbsp;&nbsp;&nbsp;<img src="https://www.mousephenotype.org/wp-content/uploads/2022/08/IMPC_logo.svg" width=100 />

## Making the tests pass

GrEBI has a suite of automated E2E tests that run the full pipeline on small synthetic datasets and compare the resulting Neo4j/Postgres database contents against committed expected output in `tests/expected_output/`. If code changes alter the pipeline output such that it no longer matches the expected snapshots, the CI will fail and you will need to update the expected output.

There are four test subgraphs, each exercising a different aspect of the pipeline:

| Test subgraph | Purpose |
| --- | --- |
| `test_clique_merge` | Verifies equivalent entities are merged into a single clique |
| `test_edge_linking` | Verifies property values referencing other entities become graph edges |
| `test_multi_datasource` | Verifies merging data from two separate datasources |
| `test_type_hierarchy` | Verifies type superclass propagation through `rdfs:subClassOf` |

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
