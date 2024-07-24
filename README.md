# GrEBI (Graphs@EBI)

EBI Codon HPC pipeline for building integrated knowledge graphs from [EMBL-EBI resources](https://www.ebi.ac.uk/services/data-resources-and-tools), so far:

* [IMPC](https://www.mousephenotype.org/)
* [GWAS Catalog](https://www.ebi.ac.uk/gwas)
* [OLS](https://www.ebi.ac.uk/ols4)
* [Reactome](https://reactome.org/)
* [OpenTargets](https://www.opentargets.org/)
* [Metabolights](https://www.ebi.ac.uk/metabolights)

GrEBI also imports complementary datasets, so far:

* [MONARCH Initiative KG](https://monarch-initiative.github.io/monarch-ingest/Sources/)
* [Ubergraph](https://github.com/INCATools/ubergraph)
* [Human Reference Atlas KG](https://humanatlas.io/)

The resulting graphs can be downloaded from https://ftp.ebi.ac.uk/pub/databases/spot/kg/

## Implementation

The pipeline is implemented as [Rust](https://www.rust-lang.org/) programs with simple CLIs, orchestrated with [Nextflow](https://www.nextflow.io/).

The primary output the pipeline is a [property graph](https://docs.oracle.com/en/database/oracle/property-graph/22.2/spgdg/what-are-property-graphs.html) for [Neo4j](https://github.com/neo4j/neo4j). The input format (after ingests to extract from [KGX](https://github.com/biolink/kgx), RDF, and bespoke DB formats) is simple [JSONL](https://jsonlines.org/) files, to which "bruteforce" integration is applied:

* All strings that begin with any IRI or CURIE prefix from the [Bioregistry](https://bioregistry.io/) are canonicalised to the standard CURIE form
* All property values that are the identifier of another node in the graph become edges
* Cliques of equivalent nodes are merged into single nodes
* Cliques of equivalent properties are merged into single properties (and for ontology-defined properties, the [qualified safe labels](https://github.com/VirtualFlyBrain/neo4j2owl/blob/master/README.md) are used)

In addition to Neo4j, the nodes and edges are loaded into [Solr](https://solr.apache.org/) for full-text search and [RocksDB](https://rocksdb.org/) for id->object resolution.



