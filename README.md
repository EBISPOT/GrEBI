# GrEBI (Graphs@EBI)

HPC pipeline using ontologies and LLM embeddings to aggregate knowledge graphs from [EMBL-EBI resources](https://www.ebi.ac.uk/services/data-resources-and-tools), the [MONARCH Initiative](https://monarch-initiative.github.io/monarch-ingest/Sources/), [DisMech](https://dismech.monarchinitiative.org/), [ROBOKOP](https://robokop.renci.org/), [Ubergraph](https://github.com/INCATools/ubergraph), and other sources.

The aim is to make it easier for humans and machines to perform integrative queries which span multiple biomedical resources, in contrast to existing REST APIs which are typically constrainted to one resource.

A development server with the output of this pipeline can be accessed at https://wwwdev.ebi.ac.uk/kg

MCP endpoint: https://wwwdev.ebi.ac.uk/kg/api/v1/mcp (Streamable HTTP)

The GrEBI pipeline is being applied to a number of projects including the [International Mouse Phenotyping Consortium (IMPC)](https://www.mousephenotype.org/) knowledge graph and the [EMBL Human Ecosystems Transversal Theme (HETT)](https://www.embl.org/about/info/human-ecosystems/) ExposomeKG.

<img src="https://www.embl.org/guidelines/design/wp-content/uploads/2022/02/EMBL_logo_colour_DIGITAL.png" width=100 />&nbsp;&nbsp;&nbsp;&nbsp;<img src="https://monarch-initiative.github.io/monarch-ingest/images/monarch-initiative.png" width=100 />&nbsp;&nbsp;&nbsp;&nbsp;<img src="https://www.mousephenotype.org/wp-content/uploads/2022/08/IMPC_logo.svg" width=100 />

## Outputs

GrEBI has two main outputs: **(1) materialised queries** and **(2) database exports**.

### 1. Materialised queries

We run a collection of graph queries periodically at EBI and materialise the results as tables, which can be loaded using standard data processing libraries such as Pandas or Polars.

* The **input Cypher queries** are stored in YAML files in the [materialised_queries](https://github.com/EBISPOT/GrEBI/tree/dev/materialised_queries) directory of this repository
* The **output CSV files** are uploaded to our FTP server in the `query_results` directories. For example, the latest materialised query results for the `impc_x_gwas` graph can be found at [https://ftp.ebi.ac.uk/pub/databases/spot/kg/impc_x_gwas/latest/query_results/](https://ftp.ebi.ac.uk/pub/databases/spot/kg/impc_x_gwas/latest/query_results/).

The results are stored in gzipped CSV files which can be loaded using Pandas:

    df = pd.read_csv('impc_x_gwas.results.csv.gz', dtype=str)

### 2. Database exports

The resulting exports can be downloaded from https://ftp.ebi.ac.uk/pub/databases/spot/kg/

| Name | Description | # Nodes | # Edges | Neo4j DB size
| ---------- | ------ | --- | --- | --- |
| `ebi_monarch_xspecies` | All datasources with cross-species phenotype matches merged | ~130m | ~850m | ~900 GB |
| `ebi_monarch` | All datasources with cross-species phenotype matches separated | | | |
| `impc_x_gwas` | Limited to data from IMPC, GWAS Catalog, OpenTargets, and related ontologies and mappings | ~30m |  ~184m |  |

Note that the purpose of this pipeline is not to supply another knowledge graph, but to facilitate querying and analysis across existing ones. Consequently the above exports should be considered temporary and are subject to be removed and/or replaced with new ones without warning.

## Running your own queries

1. Choose carefully where you would like to run Neo4j. This could be locally, on a server, or on a HPC cluster depending on which export you would like to query. You will need plenty of disk space (see above) and at least 32 GB RAM. More complex queries will require more RAM.

2. Download and extract the Neo4j export. For example to download the latest `impc_x_gwas` export:

```
curl https://ftp.ebi.ac.uk/pub/databases/spot/kg/impc_x_gwas/latest/impc_x_gwas_neo4j.tgz | tar xzf -
```

3. Start a **Neo4j 2025.03.0-community** server from the extracted folder. You can do this easily using Docker:

```
docker run -p 7474:7474 -p 7687:7687 -v $(pwd)/data:/data -e NEO4J_AUTH=none neo4j:2025.03.0-community
```

Your graph should now be accessible on port 7474. For example if you are running locally [http://localhost:7474](http://localhost:7474). You should now also be able to try out some of the [Jupyter notebooks](https://github.com/EBISPOT/GrEBI/tree/dev/notebooks).

### Running on HPC

The exact instructions will vary depending on your HPC environment. At EBI we use Slurm and Singularity. If your cluster is similar you should be able to modify the following instructions to get started.

1. Start a shell on a Slurm worker with appropriate resources:

```
srun --pty --time 1-0:0:0 -c 32 --mem 300g bash
```

2. Download and extract Neo4j as shown above, ideally to local flash-based storage. If you have a very large HPC node you may be able to extract Neo4j to ramdisk e.g. `/dev/shm` for maximum performance.

3. Find out the hostname of the worker so we can connect to it later:

```
hostname
```

4. Start Neo4j with Singularity:

```
mkdir -p neo4j_plugins tmp_neo && \
singularity run \
--bind "$(pwd)/data:/data" \
--bind "neo4j_plugins:/var/lib/neo4j/plugins" \
--writable-tmpfs \
--tmpdir tmp_neo \
--env NEO4J_AUTH=none \
--env NEO4J_server_memory_heap_initial__size=120G \
--env NEO4J_server_memory_heap_max__size=120G \
--env NEO4J_server_memory_pagecache_size=60G \
--env NEO4J_dbms_memory_transaction_total_max=60G \
--env NEO4J_apoc_export_file_enabled=true \
--env NEO4J_apoc_import_file_enabled=true \
--env NEO4J_apoc_import_file_use__neo4j__config=true \
--env NEO4J_dbms_security_procedures_unrestricted=apoc.* \
--env TINI_SUBREAPER=true \
--env NEO4J_PLUGINS=[\"apoc\"] \
docker://ghcr.io/ebispot/grebi_neo4j_with_extras:2025.03.0-community
```

Now you should be able to connect to Neo4j at the host shown earlier by `hostname`.

## Running the pipeline locally

Requires Docker. The helper scripts run Nextflow inside `ghcr.io/ebispot/grebi_nextflow:latest` and mount the repo and downloads folder.

1) Download input files

```bash
export GREBI_SUBGRAPH=disease_ontologies
./dataload/scripts/download_local.sh
```

2) Run the dataload pipeline

```bash
export GREBI_SUBGRAPH=disease_ontologies
./dataload/scripts/dataload_local.sh
```

Outputs will be under [out/disease_ontologies](out/disease_ontologies). The pipeline will also run materialised queries and package DB exports by default.

## Mapping sets used
 
The following mapping tables are loaded:

- https://data.monarchinitiative.org/mappings/latest/gene_mappings.sssom.tsv
- https://data.monarchinitiative.org/mappings/latest/hp_mesh.sssom.tsv
- https://data.monarchinitiative.org/mappings/latest/mesh_chebi_biomappings.sssom.tsv
- https://data.monarchinitiative.org/mappings/latest/mondo.sssom.tsv
- https://data.monarchinitiative.org/mappings/latest/umls_hp.sssom.tsv
- https://data.monarchinitiative.org/mappings/latest/upheno-cross-species.sssom.tsv
- https://data.monarchinitiative.org/mappings/latest/upheno-species-independent.sssom.tsv
- https://data.monarchinitiative.org/mappings/latest/nbo-go.sssom.tsv
- https://data.monarchinitiative.org/mappings/latest/uberon.sssom.tsv
- https://raw.githubusercontent.com/obophenotype/upheno-dev/refs/heads/master/src/mappings/upheno-oba.sssom.tsv
- https://raw.githubusercontent.com/mapping-commons/disease-mappings/refs/heads/main/mappings/mondo_hp_lexical.sssom.tsv
- https://raw.githubusercontent.com/mapping-commons/mh_mapping_initiative/master/mappings/mp_hp_mgi_all.sssom.tsv
- https://raw.githubusercontent.com/obophenotype/bio-attribute-ontology/master/src/mappings/oba-efo.sssom.tsv
- https://raw.githubusercontent.com/obophenotype/bio-attribute-ontology/master/src/mappings/oba-vt.sssom.tsv
- https://raw.githubusercontent.com/biopragmatics/biomappings/refs/heads/master/src/biomappings/resources/mappings.tsv

In all of the currently configured outputs, `skos:exactMatch` mappings are used for clique merging. In `ebi_monarch_xspecies`, `semapv:crossSpeciesExactMatch` is used for clique merging (so e.g. corresponding HP and MP terms will share a graph node). As this is not always desirable, a separate graph `ebi_monarch` is also provided where `semapv:crossSpeciesExactMatch` mappings are represented as edges.

## Full list of datasources

| Datasource | Loaded from |
| ---------- | ------ |
| [IMPC](https://www.mousephenotype.org/) | EBI
| [GWAS Catalog](https://www.ebi.ac.uk/gwas) | EBI
| [OLS](https://www.ebi.ac.uk/ols4) | EBI
| [OpenTargets](https://www.opentargets.org/) | EBI
| [Metabolights](https://www.ebi.ac.uk/metabolights) | EBI
| [ChEMBL](https://www.ebi.ac.uk/chembl/) | EBI
| [Reactome](https://reactome.org/) | EBI, MONARCH
| [BGee](https://www.bgee.org/about/) | MONARCH
| [BioGrid](https://thebiogrid.org/) | MONARCH
| [Gene Ontology (GO) Annotation Database](https://monarch-initiative.github.io/monarch-ingest/Sources/go/) | MONARCH
| [HGNC (HUGO Gene Nomenclature Committee)](https://www.genenames.org/) | MONARCH
| [Human Phenotype Ontology Annotations (HPOA)](https://hpo.jax.org/data/annotations) | MONARCH
| [NCBI Gene](https://monarch-initiative.github.io/monarch-ingest/Sources/ncbi/) | MONARCH
| [PHENIO](https://monarch-initiative.github.io/monarch-ingest/Sources/phenio/) | MONARCH
| [PomBase](https://www.pombase.org) | MONARCH
| [ZFIN](https://monarch-initiative.github.io/monarch-ingest/Sources/zfin/) | MONARCH
| [MedGen](https://www.ncbi.nlm.nih.gov/mesh/) | [MONARCH](https://github.com/monarch-initiative/medgen)
| [Protein ANalysis THrough Evolutionary Relationships (PANTHER)](http://pantherdb.org/) | MONARCH, ROBOKOP
| [STRING](https://string-db.org/) | MONARCH, ROBOKOP
| [Comparative Toxicogenomics Database (CTD)](http://ctdbase.org/about/) | MONARCH, ROBOKOP
| [Alliance of Genome Resources](https://www.alliancegenome.org/) | MONARCH, ROBOKOP
| [BINDING](https://www.bindingdb.org/) | ROBOKOP
| [CAM KG](https://robokop.renci.org/api-docs/docs/automat/cam-kg) | ROBOKOP
| [The Comparative Toxicogenomics Database (CTD)](http://ctdbase.org/about/) | ROBOKOP
| [Drug Central](https://drugcentral.org/) | ROBOKOP
| [The Alliance of Genome Resources](https://www.alliancegenome.org/) | ROBOKOP
| [The Genotype-Tissue Expression (GTEx) portal](https://gtexportal.org/home) | ROBOKOP
| [Guide to Pharmacology database (GtoPdb)](http://www.guidetopharmacology.org) | ROBOKOP
| [Hetionet](https://het.io/) | ROBOKOP
| [HMDB](https://hmdb.ca/) | ROBOKOP
| [Human GOA](https://www.ebi.ac.uk/GOA/index) | ROBOKOP
| [Integrated Clinical and Environmental Exposures Service (ICEES) KG](https://github.com/NCATSTranslator/Translator-All/wiki/Exposures-Provider-ICEES) | ROBOKOP
| [IntAct](https://www.ebi.ac.uk/intact/home) | ROBOKOP
| [Protein ANalysis THrough Evolutionary Relationships (PANTHER)](http://pantherdb.org/) | ROBOKOP
| [Pharos](https://pharos.nih.gov/) | ROBOKOP
| [STRING](https://string-db.org/) | ROBOKOP
| [Text Mining Provider KG](https://github.com/NCATSTranslator/Translator-All/wiki/Text-Mining-Provider) | ROBOKOP
| [Viral Proteome](https://www.ebi.ac.uk/GOA/proteomes) | ROBOKOP
| [AOPWiki](https://aopwiki.org/) | [AOPWikiRDF](https://github.com/marvinm2/AOPWikiRDF)
| [Ubergraph](https://github.com/INCATools/ubergraph)
| [MeSH](https://www.ncbi.nlm.nih.gov/mesh/)
| [Human Reference Atlas KG](https://humanatlas.io/)


## Making the tests pass

GrEBI has a suite of automated E2E tests that run the full pipeline on small synthetic datasets and compare the resulting Neo4j/Solr database contents against committed expected output in `tests/expected_output/`. If code changes alter the pipeline output such that it no longer matches the expected snapshots, the CI will fail and you will need to update the expected output.

There are four test subgraphs, each exercising a different aspect of the pipeline:

| Test subgraph | Purpose |
| --- | --- |
| `test_clique_merge` | Verifies equivalent entities are merged into a single clique |
| `test_edge_linking` | Verifies property values referencing other entities become graph edges |
| `test_multi_datasource` | Verifies merging data from two separate datasources |
| `test_type_hierarchy` | Verifies type superclass propagation through `rdfs:subClassOf` |

### Prerequisites

You need Docker with the `docker compose` plugin and enough disk space to build two images. Build them locally before running the tests:

    docker build -t ghcr.io/ebispot/grebi_dataload:dev -f dataload/Dockerfile dataload
    docker build -t ghcr.io/ebispot/grebi_combined:dev -f webapp/Dockerfile.combined .

### Running all tests

Run the full E2E test suite across all four test subgraphs:

    bash tests/run_all_e2e.sh

This will run each test subgraph through the full Nextflow pipeline (ingest → assign IDs → merge → index → link → create Neo4j → run queries → create Solr → integration tests), export DB snapshots, and compare them against `tests/expected_output/`.

### Running a single test

To run only one test subgraph:

    bash tests/run_e2e.sh test_clique_merge

### Updating expected output

When your changes intentionally alter the pipeline output, you need to update the expected snapshots. Run the pipeline for the affected test subgraph, inspect the changes, and commit them:

    export GREBI_SUBGRAPH=test_clique_merge
    export GREBI_NF_EXTRA_ARGS="--export_snapshots true"
    bash dataload/scripts/dataload_local.sh

Copy the new snapshots to expected output:

    cp out/test_clique_merge/test_clique_merge_snapshot_*.jsonl \
       tests/expected_output/test_clique_merge/

Now inspect the changes with `git diff` and make sure they are intentional. When you are happy, stage and commit the updated expected output:

    git add -A tests/expected_output/
    git commit -m "Update expected test output"

## Documentation

User-facing documentation lives in `docs/` as Markdown files. The sidebar ordering is defined in `docs/_sidebar.yaml`. The same content is served in the web UI (via the **Docs** tab) and can be exported as a PDF.

### Editing docs

Edit or add `.md` files in `docs/`. Use standard Markdown plus two custom tags for interactive examples:

- `<api-example method="GET" url="/api/v1/..." params='{"key":"value"}' />` — generic REST endpoint
- `<query-template id="query_id" graph="graph_name" params='{"param":"value"}' />` — query template (uses the same pandas/CSV code snippets shown on the query pages)

After editing, copy the updated files to the UI bundle:

```bash
cp -r docs/* webapp/grebi_ui/docs/
```

### Generating the PDF locally

The PDF generator requires a running API stack and uses Node (with puppeteer + marked + prismjs).

```bash
# 1. Start the stack (if not already running)
cd out/dismech/dismech && ./grebi.sh -api -ui &

# 2. Install dependencies (first time only)
cd webapp/grebi_ui && npm install && cd ..

# 3. Generate the PDF
cd webapp
node generate_docs_pdf.mjs \
  --api-url http://localhost:8090 \
  --docs-dir ../docs \
  --output ../tmp/grebi_docs.pdf
```

The generator fetches live API responses for all `<api-example>` and `<query-template>` tags. If the API is not running, code snippets are still included but response sections show a placeholder.

## Implementation

The pipeline is implemented as [Rust](https://www.rust-lang.org/) programs with simple CLIs, orchestrated with [Nextflow](https://www.nextflow.io/). Input KGs are represented in a variety of formats including [KGX](https://github.com/biolink/kgx), [RDF](https://www.w3.org/RDF/), and [JSONL](https://jsonlines.org/) files. After loading, a simple "bruteforce" integration strategy is applied:

* All strings that begin with any IRI or CURIE prefix from the [Bioregistry](https://bioregistry.io/) are canonicalised to the standard CURIE form
* All property values that are the identifier of another node in the graph become edges
* Cliques of equivalent nodes are merged into single nodes
* Cliques of equivalent properties are merged into single properties (and for ontology-defined properties, the [qualified safe labels](https://github.com/VirtualFlyBrain/neo4j2owl/blob/master/README.md) are used)




