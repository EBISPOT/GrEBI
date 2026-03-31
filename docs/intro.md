
# What is GrEBI?

**GrEBI** (Graphs@EBI) is a website, API, and MCP server which makes it easier for researchers and their LLM agents to perform integrative queries which span multiple biomedical resources, in contrast to existing REST APIs which are typically designed to query one resource at a time.

GrEBI was developed to support many different research projects, including the [EMBL Human Ecosystems Transversal Theme](https://www.embl.org/about/info/human-ecosystems/), the [MONARCH Initiative](https://monarchinitiative.org/), the [NIH National Human Genome Research Institute Phenomics First Resource](https://reporter.nih.gov/project-details/10448140), the [Human Reference Atlas](https://humanatlas.io/), the [NHGRI-EBI GWAS Catalog](https://www.ebi.ac.uk/gwas/), the [International Mouse Phenotyping Consortium (IMPC)](https://www.mousephenotype.org/), and the [OpenTargets Platform](https://platform.opentargets.org/).


## What is GrEBI useful for?

In one example scenario, the [GWAS Catalog](https://www.ebi.ac.uk/gwas/) contains human gene-phenotype associations, and [IMPC](https://www.mousephenotype.org/) contains mouse gene-phenotype associations. Together these data can be used to identify shared mechanism of disease across species. However they are maintained in different databases, by different communities, and using different ontologies for annotation.

LLM agents are able to query examples from both in small requests and combine results, but can't change how the APIs allow us query the underlying data. For example, an agent can use the GWAS or IMPC APIs to retrieve information about one gene at a time or search for genes by name, but it can't perform large-scale all-by-all queries such as retrieving _all_ of the mouse-human orthologs which share similar phenotypes because the GWAS Catalog and IMPC APIs don't provide this functionality.

In database terminology, this would constitute a **`JOIN`** to connect genes and/or phenotypes across the two resources. A bioinformatician might implement this as a one-time fix by dowloading data and making a GWAS-IMPC table. This is essentially what GrEBI's knowledge graph does, but it makes joins between _all_ of the data, at scale using the EBI HPC, and it updates on a regular release schedule [currently every week].

## Mappings and embeddings

One of the biggest challenges in biomedical data integration is <b>semantic heterogeneity</b>, where different databases use different semantics for the same or similar concepts. For example, `HP:0002240 Hepatomegaly` is used to annotate human studies in the GWAS Catalog, while `MP:0000599 enlarged liver` is used to annotate mouse studies in IMPC. A biologist would know these are related, but to a computer they appear completely different. To address this, expert curators build mapping tables, such as the tables in [mapping commons](https://github.com/mapping-commons) and [SeMRA](https://github.com/biopragmatics/semra). GrEBI loads these mapping tables and uses them to merge exact matches into single graph nodes. 

In some cases these mappings are missing, especially when the relationship between concepts is more distant. For example, an IMPC study annotated with `MP:0003179 Thrombocytopenia` may be relevant to a GWAS Catalog study annotated with `EFO:0004309 platelet count`. However, these are different concepts; one is a phenotype and one is a measurement. To address this, GrEBI also loads **embedding vectors** from the [Ontology Lookup Service (OLS)](https://www.ebi.ac.uk/ols4/)<pubmed id="40323307"/>. These vectors are derived from embedding information about ontology terms in large embedding models, including [`llama-embed-nemotron-8b`](https://huggingface.co/nvidia/llama-embed-nemotron-8b) and [`text-embedding-3-small`](https://developers.openai.com/api/docs/models/text-embedding-3-small), and the distance between the vectors can be used in graph queries to connect between semantically similar nodes. For example, `Thrombocytopenia` and the `platelet count` are ~61% similar according to `text-embedding-3-small`.

## How can I use GrEBI?

GrEBI provides many pre-baked graph queries which can be used to get data. Each query has an input, which you provide, and some outputs which are returned as simple tables. You can use these queries yourself in the browser, or programatically from Python or R, or your LLM agents can use GrEBI as an MCP server. For example:

<query-template id="disease_to_treatments" graph="dismech" disease_id="mondo:0005002" />