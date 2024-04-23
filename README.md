# GrEBI (Graphs@EBI)

EBI Codon HPC pipeline for building integrated knowledge graphs from [EMBL-EBI resources](https://www.ebi.ac.uk/services/data-resources-and-tools), so far:

* [IMPC](https://www.mousephenotype.org/)
* [GWAS Catalog](https://www.ebi.ac.uk/gwas)
* [OLS](https://www.ebi.ac.uk/ols4)
* [Reactome](https://reactome.org/)
* [OpenTargets](https://www.opentargets.org/)

GrEBI also imports complementary datasets, so far:

* [MONARCH Initiative KG](https://monarch-initiative.github.io/monarch-ingest/Sources/)
* [Ubergraph](https://github.com/INCATools/ubergraph)

The resulting graphs can be downloaded from https://ftp.ebi.ac.uk/pub/databases/spot/kg/

## Implementation

This is a implemented as a combination of Python scripts for orchestration and [Rust](https://www.rust-lang.org/) programs with simple CLIs for high performance data wrangling.

The pipeline makes property graphs (where both nodes and edges can have properties) for Neo4j, with "bruteforce" integration:

* All strings that begin with any IRI or CURIE prefix from the [Bioregistry](https://bioregistry.io/) are canonicalised to the standard CURIE form
* All property values that are the identifier of another node in the graph become edges
* Cliques of equivalent nodes are merged into single nodes




