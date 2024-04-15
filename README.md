# GrEBI (Graphs@EBI)

EBI Codon HPC pipeline for building integrated knowledge graphs from EBI resources, so far:

* [IMPC](https://www.mousephenotype.org/)
* [GWAS Catalog](https://www.ebi.ac.uk/gwas)
* [OLS](https://www.ebi.ac.uk/ols4)
* [Reactome](https://reactome.org/)

This is a implemented as a combination of Python scripts for orchestration and [Rust](https://www.rust-lang.org/) programs with simple CLIs for high performance data wrangling. It uses [RocksDB](https://github.com/facebook/rocksdb) as an intermediate database.

The pipeline makes property graphs (where both nodes and edges can have properties) for Neo4j, with "bruteforce" integration:

* All strings that begin with any IRI or CURIE prefix from the [Bioregistry](https://bioregistry.io/) are canonicalised to the standard CURIE form
* All property values that are the identifier of another node in the graph become edges
* Cliques of equivalent nodes are merged into single nodes

The resulting graphs can be downloaded from https://ftp.ebi.ac.uk/pub/databases/spot/kg/






