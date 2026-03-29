
# GrEBI Dataload

The GrEBI dataload is implemented as a [Nextflow](https://www.nextflow.io/) pipeline. It takes about 15 minutes on an M3 MacBook Air to build the `dismech` graph with ~80k nodes and ~3 million edges, or a day on the EBI HPC to build the `ebi_monarch_xspecies` graph with >50 million nodes and >1 billion edges.

GrEBI runs on four databases: Neo4j, PostgreSQL, Solr, and SQLite. Each database has a different purpose:

* Postgres is used by the backend to drive most of the API endpoints used by the website. It stores nodes and edges with minimal metadata, and embedding vectors with pgvector.
* Neo4j is used by the `grebi_cypher_service` to drive Cypher queries. It stores nodes and edges with minimal metadata.
* Solr drives the free text lexical search. It stores nodes with minimal metadata and also has an autocomplete list derived from all of the names in the graph.
* SQLite is used as a key value store to back the `grebi_resolver_service`. The resolver service maps node and edge IDs to compressed binary blobs containing their complete set of properties stored as JSON.

> **Why do we duplicate the data with `grebi_resolver_service`?** All of the information GrEBI has about a node can be multiple MB, which adds up quickly. The website therefore shows minimal metadata in search results, which it can retrieve from Postgres, Neo, or Solr. Then it uses the resolved object for the full page (e.g. viewing an individual node with all of its properties). 





