# GrEBI (Graphs@EBI)

HPC pipeline to aggregate and integrate knowledge graphs from [EMBL-EBI resources](https://www.ebi.ac.uk/services/data-resources-and-tools), the [MONARCH Initiative KG](https://monarch-initiative.github.io/monarch-ingest/Sources/), [ROBOKOP](https://robokop.renci.org/), and other resources.

| Datasource | Loaded from |
| ---------- | ------ |
| [IMPC](https://www.mousephenotype.org/) | EBI
| [GWAS Catalog](https://www.ebi.ac.uk/gwas) | EBI
| [OLS](https://www.ebi.ac.uk/ols4) | EBI
| [Reactome](https://reactome.org/) | EBI
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
| [Human Reference Atlas KG](https://humanatlas.io/)

The resulting graphs can be downloaded from https://ftp.ebi.ac.uk/pub/databases/spot/kg/ebi/

## Implementation

The pipeline is implemented as [Rust](https://www.rust-lang.org/) programs with simple CLIs, orchestrated with [Nextflow](https://www.nextflow.io/).

The primary output the pipeline is a [property graph](https://docs.oracle.com/en/database/oracle/property-graph/22.2/spgdg/what-are-property-graphs.html) for [Neo4j](https://github.com/neo4j/neo4j). The input format (after ingests to extract from [KGX](https://github.com/biolink/kgx), RDF, and bespoke DB formats) is simple [JSONL](https://jsonlines.org/) files, to which "bruteforce" integration is applied:

* All strings that begin with any IRI or CURIE prefix from the [Bioregistry](https://bioregistry.io/) are canonicalised to the standard CURIE form
* All property values that are the identifier of another node in the graph become edges
* Cliques of equivalent nodes are merged into single nodes
* Cliques of equivalent properties are merged into single properties (and for ontology-defined properties, the [qualified safe labels](https://github.com/VirtualFlyBrain/neo4j2owl/blob/master/README.md) are used)

In addition to Neo4j, the nodes and edges are loaded into [Solr](https://solr.apache.org/) for full-text search and [RocksDB](https://rocksdb.org/) for id->object resolution.



