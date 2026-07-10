
# Deploying GrEBI

GrEBI is [open source on GitHub](https://github.com/EBISPOT/GrEBI) under the Apache 2.0 license. It is maintained by the [Samples, Phenotypes, and Ontologies (SPOT)](https://www.ebi.ac.uk/spot/) team at [EMBL-EBI](https://www.ebi.ac.uk/).

The GrEBI dataload is implemented as a containerised [Nextflow](https://www.nextflow.io/) pipeline, with data processing in a combination of small Rust programs and Python scripts. The GrEBI stack (API, MCP server, website) is implemented in Java and TypeScript/React, and can run in various containerised configurations (single Docker container, Docker compose, or Kubernetes).


## Loading data into GrEBI

The GrEBI Nextflow pipeline takes about 15 minutes on an M3 MacBook Air to build the `dismech` graph with ~80k nodes and ~3 million edges, or a day on the EBI HPC to build the `ebi_monarch_xspecies` graph with >50 million nodes and >1 billion edges.

## Releases and downloads

A completed build produces a release bundle (assembled by the `construct_release`/`package_release` pipeline steps): per-subgraph Neo4j store archives (`<subgraph>_neo4j.tar.xz`), the Postgres store (`postgres.tar.xz`), `<subgraph>_metadata.json`, materialised `query_results/`, and a generated `grebi_dev.sh` that starts the stack from the extracted files.

Full multi-source builds run weekly on the EBI Codon HPC and are published two ways:

- **EBI FTP** — [`ebi_datarelease_to_ftp.sh`](https://github.com/EBISPOT/GrEBI/blob/dev/dataload/scripts/ebi_datarelease_to_ftp.sh) copies the archives to a date-versioned directory (and a `latest/` alias) under `https://ftp.ebi.ac.uk/pub/databases/spot/kg/`. This is the home of the multi-gigabyte store archives.
- **GitHub Releases** — [`datarelease_to_github.sh`](https://github.com/EBISPOT/GrEBI/blob/dev/dataload/scripts/datarelease_to_github.sh) creates a dated GitHub Release (tag `data-<version>`) and attaches the small, self-describing artefacts (metadata, packaged query results, run script, and a `SHA256SUMS`). GitHub caps each asset at 2 GiB, so by default the large store archives are not uploaded — the release notes link to their FTP copies instead. Set `GREBI_GH_SPLIT=1` to instead split oversized archives into <2 GiB parts and upload every part. It needs only outbound HTTPS and a `GITHUB_TOKEN` with `contents: write`, so it can run from a Codon login node right after the FTP step; run it with `GREBI_GH_DRY_RUN=1` first to preview the plan.

For testing and demos, the [`kg_release.yml`](https://github.com/EBISPOT/GrEBI/blob/dev/.github/workflows/kg_release.yml) GitHub Actions workflow builds a small subgraph (default `dismech`) entirely in CI and publishes its `release.tar.xz` as a GitHub Release, so a downloadable KG artefact is always available without HPC access.

## Downloads and Ingests

GrEBI datasources are defined in YAML files in the [`configs/datasource_configs`](https://github.com/EBISPOT/GrEBI/tree/dev/configs/datasource_configs) directory. For example, [`otar_disease_phenotype.yaml`](https://github.com/EBISPOT/GrEBI/blob/dev/configs/datasource_configs/otar/otar_disease_phenotype.yaml) defines a datasource to import disease-phenotype associations from the Open Targets platform:

```yaml
id: OpenTargets
enabled: true
description: "Disease-to-phenotype associations derived from the Open Targets evidence pipeline"
download:
- dest: otar/disease_phenotype/
    sources:
    - https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/latest/output/disease_phenotype/disease_phenotype.parquet
ingests:
- globs: ["otar/disease_phenotype/*.parquet"]
    command: '
    cat $GREBI_INGEST_FILENAME |
    grebi_parquet2jsonl |
    grebi_nodes2edges --from-field disease --to-field phenotype --edge-type biolink:has_phenotype |
    grebi_transform_jsonl
        --json-inject-type biolink:Disease
        --json-inject-key-prefix otar:
        '
```

The `download` section defines which files are needed, and where to download them. Each file can have multiple sources and the pipeline will try each source in turn (for example, many of the datasources have both an NFS path used on the EBI HPC and a URL to fall back on when running elsewhere).

The `ingests` section defines preprocessing needed before the file is loaded into GrEBI.

## The GrEBI datamodel

Data to load into GrEBI should be structured as JSON objects, one per line. Each JSON line represents a node, and its properties are the properties on the node.

GrEBI does not have opinions about the schema of your data, and it doesn't have much of a schema itself apart from a few reserved `grebi:` properties for reification. Most of the data loaded in GrEBI comes from sources with their own schema and validation, for example ontologies using OWL2 or KGs using [LinkML](https://linkml.io/). GrEBI is just the querying engine and does not do any validation.

### Identifier normalisation

GrEBI normalises all identifiers using the [Bioregistry](https://bioregistry.io/). This normalisation is applied to **all strings**: all of the keys and values in all JSON objects. So if something looks like an identifier, it gets normalised. For example, BioRegistry defines `SwissProt:`, `UniProt:`, `UniProtKB`, `Uniprot ID`, `uniprot/swiss-prot`, and `UP` as alternative prefixes for UniProt, which it assigns the `uniprot:` prefix. Any string that begins with any of these prefixes will be normalised to `uniprot:`.

### Clique merging

Each JSON object should have one or more identifier defined using one of the properties in `identifier_props`, configurable by graph; `id` is conventionally used, but the `ebi_monarch_xspecies` graph also adds `skos:exactMatch` and `semapv:crossSpeciesExactMatch` in its config file [ebi_monarch_xspecies.json](https://github.com/EBISPOT/GrEBI/blob/dev/configs/subgraph_configs/ebi_monarch_xspecies.json). All JSON objects which have any identifiers in common, after identifier normalisation, will be merged into the same graph node.

Additional cliques to be merged can be defined in the graph config under `additional_equivalence_groups`. For example, the `ebi_monarch_xspecies` graph defines:

  "additional_equivalence_groups": [
    [
      "grebi:name",
      "ols:label",
      "rdfs:label",
      "monarch:name",
      "impc:name",
      "reactome:displayName",
      "dcterms:title",
      "ncit:Preferred_Name",
      "robokop:name",
      "otar:name"
    ],
    [
      "mondo:0000001",
      "ogms:0000031"
    ],
    ...

To merge (1) all of the different name properties into a single property; and (2) to merge `mondo:0000001 disease` with `ogms:0000031 disease`. There are other identifiers which also mean disease, for example `doid:4 disease`. However we don't need to put `doid:4 disease` into this group because MONDO already defines `mondo:0000001 skos:exactMatch doid:4` in the ontology, and `skos:exactMatch` is configured as an identifier property in `identifier_props`. So `doid:4` becomes an alternative identifier for `mondo:0000001`, and they both end up merged into the same graph node.

### Edges

Edges are created when a property value on one node is an identifier for another node. For example, the following JSON results in both `mondo:0005015` having a property `biolink:has_phenotype hp:0003074`, and a `biolink:has_phenotype` edge being created between `mondo:0005015` and `hp:0003074`.

```json
{ "id": "hp:0003074", "dcterms:title": "Hyperglycemia" }
{ "id": "mondo:0005015", "dcterms:title": "diabetes mellitus", "biolink:has_phenotype": "hp:0003074" }
```

> *Caveat:* Because edges in GrEBI are derived from properties, an edge A->B cannot be created without an associated property in the JSON of node A. This means that you while edges scale easily to millions of _incoming_ edges e.g. gene to associated phenotypes, they do not scale to millions of _outgoing_ edges e.g. phenotype to all the genes, because it would require millions of properties in a JSON line. Because edges in GrEBI can be traversed in either direction this is not a limitation in practice, but incoming edges should be preferred for very large numbers of relationships. Fortunately this tends to be how relationships are modelled already.

### Reification

GrEBI is a property graph where properties can have their own properties, and edges can also have properties. The special `grebi:value` and `grebi:properties` properties are reserved for this purpose. For example, this example JSON results in the `biolink:has_phenotype` property and edge both having the metadata `biolink:xref  https://www.who.int/health-topics/diabetes`. Note that the JSON is shown over multiple lines for readability, but in practice would be a single line.

```json
{
    "id": "mondo:0005015",
    "dcterms:title": "diabetes mellitus",
    "biolink:has_phenotype": {
        "grebi:value": "hp:0003074",
        "grebi:properties": {
            "biolink:xref": "https://www.who.int/health-topics/diabetes"
        }
    }
}
```

## Ingest tools

The GrEBI dataload pipeline includes a set of Rust command-line tools for ingesting, converting, and transforming data into the internal JSONL format. These tools are designed to be composed via Unix pipes.

### `grebi_transform_jsonl`

A high-throughput general purpose JSONL transformer written in Rust. Reads JSONL from stdin, applies a series of transformations, and writes JSONL to stdout.

#### Usage

```
cat input.jsonl | grebi_transform_jsonl [OPTIONS] > output.jsonl
```

#### Options

All options are optional; no options = passthrough.

| Option | Description |
|---|---|
| `--json_select_keys <KEYS>` | Comma-separated list of keys to keep. All other keys are removed. |
| `--json_remove_keys <KEYS>` | Comma-separated list of keys to remove. |
| `--json_rename <FROM:TO>` | Rename a key. Supports nested paths with `.` (e.g. `foo.bar:baz`). Can be specified multiple times. |
| `--json_inject_type <TYPE>` | Inject a `grebi:type` field with the given value. |
| `--json_inject_key_prefix <PREFIX>` | Prefix all property keys (except `id` and keys already containing `:`) with this string. Defaults to empty. |
| `--json_inject_value_prefix <KEY:PREFIX>` | Prefix all values of the given key with the given string. Can be specified multiple times. |
| `--json_de_nest_field <FIELD.SUBFIELD>` | Extract a nested field from object values. The subfield becomes the `grebi:value` and remaining fields become `grebi:properties`. Can be specified multiple times. |
| `--json_select_by_value <KEY:VALUE>` | Only output records where the given key has the given string value. Can be specified multiple times; all conditions must match. |
| `--json_inject_hashid` | Flag. Add a `grebi:hashId` field containing a SHA-1 hash of the entire JSON object. |

#### Behaviour

1. Each line of input is parsed as a JSON object.
2. If `--json_select_by_value` is specified, records not matching all conditions are discarded.
3. If `--json_inject_type` is specified, a `grebi:type` array is added.
4. Keys are filtered by `--json_select_keys` or `--json_remove_keys`.
5. Non-`id` keys without a `:` are prefixed with `--json_inject_key_prefix`.
6. Values are mapped: `--json_inject_value_prefix` prepends strings; `--json_de_nest_field` restructures nested objects.
7. If `--json_inject_hashid` is set, a SHA-1 hash is computed and stored.
8. Key renames from `--json_rename` are applied last.

### `grebi_rdf2jsonl`

Converts RDF data (XML, Turtle, N-Quads) into JSONL suitable for loading into GrEBI. Loads the entire graph into memory, resolves OWL axiom and RDF statement reifications, and writes one JSONL record per subject.

#### Usage

```
cat input.rdf | grebi_rdf2jsonl --rdf_type <TYPE> [OPTIONS] > output.jsonl
```

#### Options

| Option | Required | Description |
|---|---|---|
| `--rdf_type <TYPE>` | **Yes** | Input format. One of `rdf_triples_xml`, `rdf_triples_turtle`, or `rdf_quads_nq`. |
| `--rdf_graph <URI>` | No | Named graph(s) to load when using `rdf_quads_nq`. Can be specified multiple times. If omitted, all graphs are loaded. |
| `--nest_objects_of_predicate <URI>` | No | Predicates whose objects should be inlined (nested) into the subject rather than emitted as top-level subjects. Can be specified multiple times. |
| `--exclude_objects_of_predicate <URI>` | No | Predicates whose objects should be excluded entirely. Can be specified multiple times. |
| `--reif_pointer_predicate <URI>` | No | Predicates that point to a reification metadata object. Can be specified multiple times. |
| `--reif_predicate_predicate <URI>` | No | Predicates from a reification metadata object to the reified predicate. Can be specified multiple times. |
| `--reif_value_predicate <URI>` | No | Predicates from a reification metadata object to the reified value. Can be specified multiple times. |
| `--rdf_types_are_grebi_types` | No | Flag. If set, `rdf:type` values are also emitted as `grebi:type`. Defaults to false. |

#### Behaviour

1. The entire RDF graph is loaded into memory.
2. OWL axiom subjects (`owl:Axiom`) and RDF statement subjects (`rdf:Statement`) are identified.
3. Reification metadata is resolved: pointer → predicate → value triples are collapsed into annotated edges.
4. Objects of `--nest_objects_of_predicate` predicates are inlined and excluded from top-level output.
5. Objects of `--exclude_objects_of_predicate` and `--reif_pointer_predicate` predicates are excluded from top-level output.
6. Each remaining subject is written as a JSONL record with its properties.

### `grebi_tsv2jsonl`

Converts TSV (tab-separated values) data into JSONL. Reads from stdin and writes to stdout. Automatically skips leading comment lines starting with `#`.

#### Usage

```
cat input.tsv | grebi_tsv2jsonl [OPTIONS] > output.jsonl
```

#### Options

All options are optional.

| Option | Description |
|---|---|
| `--tsv_columns <COLUMNS>` | Comma-separated list of column names to use instead of the header row. If provided, the first line of input is treated as data. |
| `--tsv_array_delimiter <DELIM>` | If set, values in each field are split by this delimiter and emitted as JSON arrays. Without this, each field is emitted as a single-element array. |
| `--tsv_ignore_empty_fields` | Flag. Skip fields with empty string values. |

#### Behaviour

1. Lines starting with `#` at the beginning of the file are skipped (e.g. CTD header comments).
2. The first non-comment line is treated as a header row (unless `--tsv_columns` is provided).
3. Each subsequent row is emitted as a JSON object where keys are column names and values are arrays of strings.
4. If `--tsv_array_delimiter` is set, field values are split on that delimiter into multi-element arrays.

### `grebi_parquet2jsonl`

Converts Parquet data into JSONL. Reads the entire Parquet file from stdin into memory and writes line-delimited JSON to stdout using the Arrow JSON writer.

#### Usage

```
cat input.parquet | grebi_parquet2jsonl > output.jsonl
```

#### Options

None. This tool takes no arguments.

### `grebi_ingest_ols`

Ingests ontology data from the OLS (Ontology Lookup Service) JSON export format. Reads a JSON document containing ontologies with their classes, properties, and individuals from stdin and writes GrEBI JSONL to stdout.

#### Usage

```
cat ols_export.json | grebi_ingest_ols --ontologies <IDS> [OPTIONS] > output.jsonl
```

#### Options

| Option | Required | Description |
|---|---|---|
| `--ontologies <IDS>` | **Yes** | Comma-separated list of ontology IDs to include (e.g. `efo,mondo,hp`). Ontologies not in this list are skipped. |
| `--defining_only` | No | Flag. If set, only include entities that are defined by the ontology being processed (skip imported terms). |
| `--skip_obsolete` | No | Flag. If set, skip entities marked as obsolete. |

#### Environment Variables

| Variable | Description |
|---|---|
| `GREBI_DATASOURCE_ID` | If set, used as the `id` for the ontology metadata record instead of the ontology ID from the file. |

#### Behaviour

1. The input JSON is expected to have a top-level `ontologies` array.
2. Each ontology object contains metadata fields and arrays of `classes`, `properties`, and `individuals`.
3. An ontology metadata record is emitted with `grebi:type` set to `["ols:Ontology", "grebi:Datasource"]`.
4. Each class, property, and individual is emitted as a separate JSONL record with OLS-prefixed property names.
5. Ontologies not listed in `--ontologies` are skipped entirely.

### `grebi_ingest_kgx_edges`

Ingests edge data in KGX (Knowledge Graph Exchange) JSONL format. Reads KGX edge records from stdin and converts them into GrEBI edge JSONL on stdout.

Each KGX edge has `subject`, `predicate`, and `object` fields. These are mapped to a GrEBI edge where the subject becomes the `id`, the predicate becomes the edge type key, and the object becomes the `grebi:value`. All other fields are carried as edge metadata in `grebi:properties`.

#### Usage

```
cat kgx_edges.jsonl | grebi_ingest_kgx_edges [OPTIONS] > output.jsonl
```

#### Options

All options are optional.

| Option | Description |
|---|---|
| `--kgx_rename_field <FROM:TO>` | Rename a metadata field. Can be specified multiple times. |
| `--kgx_inject_key_prefix <PREFIX>` | Prefix all metadata keys (that don't already contain `:`) with this string. Defaults to empty. |

#### Behaviour

1. Each input line is parsed as a JSON object with `subject`, `predicate`, and `object` string fields.
2. Lines with missing or non-string subject/predicate/object are skipped with a warning.
3. Remaining fields are included as edge properties, with keys optionally renamed or prefixed.
4. Null values are dropped.

### `grebi_ingest_sssom`

Ingests mapping data in SSSOM (Simple Standard for Sharing Ontological Mappings) TSV format. Reads an SSSOM file from stdin and writes GrEBI edge JSONL to stdout.

SSSOM files have a YAML header embedded in `#`-prefixed comment lines, followed by a TSV body. The YAML header's `curie_map` is used to expand CURIEs into full IRIs during ingestion.

#### Usage

```
cat mappings.sssom.tsv | grebi_ingest_sssom > output.jsonl
```

#### Options

None. This tool takes no arguments.

#### Behaviour

1. Lines starting with `#` are parsed as a YAML header. The `curie_map` section is extracted and used to build a prefix map for expanding CURIEs.
2. The remaining lines are parsed as TSV with headers.
3. The `subject_id`, `predicate_id`, and `object_id` columns are used to construct edges.
4. All CURIEs (in subject, predicate, object, and metadata columns) are expanded using the prefix map from the header.
5. Each row is emitted as a GrEBI edge record where:
   - `id` is the subject
   - The predicate is the edge type key
   - The object is the `grebi:value`
   - All other columns are included in `grebi:properties`

### `grebi_nodes2edges`

Re-shapes JSON objects which represent edges into something GrEBI will recognise as edges. For each input JSONL record, it extracts the `--from_field` and `--to_field` values as the source and destination of the edge, and carries everything else as edge metadata.

If the from or to fields contain arrays, the Cartesian product of all (from × to) combinations is produced.

#### Usage

```
cat input.jsonl | grebi_nodes2edges --from_field <FIELD> --to_field <FIELD> --edge_type <TYPE> > output.jsonl
```

#### Options

All three options are required.

| Option | Description |
|---|---|
| `--from_field <FIELD>` | The field to use as the edge source (`id` in the output). |
| `--to_field <FIELD>` | The field to use as the edge target (`grebi:value` in the output). |
| `--edge_type <TYPE>` | The edge type (property name in the output) |

#### Behaviour

1. Each input record is parsed as JSON.
2. The from and to fields are extracted. If either is an array, all combinations are generated.
3. For each (from, to) pair, an output record is written with:
   - `id` set to the from value
   - A key named after `--edge_type` containing `grebi:value` (the to value) and `grebi:properties` (all remaining fields from the input).

### `grebi_unwind`

Unwinds an array field into separate JSONL records. For each element of the specified array field, a copy of the entire record is emitted with that field replaced by the single element.

#### Usage

```
cat input.jsonl | grebi_unwind --unwind_field <FIELD> > output.jsonl
```

#### Options

| Option | Required | Description |
|---|---|---|
| `--unwind_field <FIELD>` | **Yes** | The array field to unwind. |

#### Example

Given the input:

```json
{"id": "x", "tags": ["a", "b"]}
```

Running `grebi_unwind --unwind_field tags` produces:

```json
{"id": "x", "tags": "a"}
{"id": "x", "tags": "b"}
```

## Identifiers and CURIEs

GrEBI normalises every identifier to a **CURIE** (Compact URI) using the
[Bioregistry](https://bioregistry.io).

### Format

A CURIE has two parts separated by a colon:

```
prefix:localId
```

For example:

| CURIE | Meaning |
|-------|---------|
| `mondo:0005002` | COPD in the MONDO ontology |
| `hgnc:1100` | BRCA1 in the HGNC gene register |
| `hp:0001234` | A phenotype in the Human Phenotype Ontology |
| `chebi:15365` | Aspirin in ChEBI |

> **Note:** GrEBI uses **lowercase prefixes** (`hgnc`, not `HGNC`).

### Why CURIEs?

Different datasources use different schemes for the same entity:

- UniProt may reference `HGNC:1100`
- MONARCH may reference `NCBIGene:672`
- OLS may reference `ensembl:ENSG00000012048`

The GrEBI pipeline maps all of these to their **canonical CURIE** form and
groups them into a single clique. See the Data Model section for how
cliques work.

### Prefix normalisation

The prefix map is stored in PostgreSQL (the `grebi_prefix_map` table, populated
during the dataload). The backend normalises IRIs/CURIEs to their canonical
CURIE form by spawning the bundled native `grebi_reprefix` helper over stdio
(loading the map from postgres on first use), so there is no separate prefix
service to deploy. It is exposed through the API:

```bash
curl "http://localhost:8090/api/v1/normalise_curies?iris_or_curies=http://purl.uniprot.org/ensembl/ENSG00000012048"
```

```json
{"curies": ["ensembl:ENSG00000012048"]}
```



## Deploying the GrEBI stack

The dataload produces two main database artefacts used to run the GrEBI stack: Neo4j and PostgreSQL. Each database stores different views over the same data (simple JSON objects for nodes and edges), with different purposes:

* Postgres is used by the backend to drive most of the API endpoints used by the website. It stores nodes and edges with minimal metadata, embedding vectors with pgvector, graph metadata, and compressed JSON blobs used to resolve full node and edge objects. It also drives the free text lexical search and holds an autocomplete list derived from all of the names in the graph.
* Neo4j is used by the `grebi_cypher_service` to drive Cypher queries. It stores nodes and edges with minimal metadata.

### Kubernetes with managed PostgreSQL

The Helm chart under `webapp/k8chart` expects PostgreSQL to be provided externally. The backend reads its connection settings from a Kubernetes secret containing these environment variables:

* `GREBI_POSTGRES_HOST`
* `GREBI_POSTGRES_PORT`
* `GREBI_POSTGRES_DB`
* `GREBI_POSTGRES_USER`
* `GREBI_POSTGRES_PASSWORD`
* `GREBI_POSTGRES_SSLMODE` (optional, for example `require`)
* `GREBI_POSTGRES_JDBC_PARAMS` (optional extra JDBC query parameters)

By default the chart looks for a secret named `<release-name>-postgres`. You can create it ahead of time:

```bash
kubectl create secret generic grebi-postgres \
  --from-literal=GREBI_POSTGRES_HOST=pgsql-hlvm-138 \
  --from-literal=GREBI_POSTGRES_PORT=5432 \
  --from-literal=GREBI_POSTGRES_DB=spotefoexp \
  --from-literal=GREBI_POSTGRES_USER=spot \
  --from-literal=GREBI_POSTGRES_PASSWORD='<password>' \
  --from-literal=GREBI_POSTGRES_SSLMODE=require
```

Then either name your Helm release `grebi`, or set `postgres.secret.name` to the secret name you want the chart to reference. The chart does not create this secret for you.
