
# Using GrEBI

## Navigating the graph

Each node in GrEBI represents a **clique**: a group of identifiers that have
been determined to refer to the same real-world entity. For example, the gene
*BRCA1* may appear as `hgnc:1100`, `ncbigene:672`, and `ensembl:ENSG00000012048`.
GrEBI merges these into a single node with one **canonical ID** and links to all
source IDs.

Edges connect two clique nodes. Each edge carries:

| Property | Description |
|----------|-------------|
| `edge_id` | Unique identifier for this edge |
| Type label | A Biolink predicate, e.g. `biolink:related_to` |
| Provenance | The datasource(s) that asserted the relationship |

Nodes are labelled with one or more **Biolink types** such as
`biolink:Gene`, `biolink:Disease`, or `biolink:PhenotypicFeature`.
The type hierarchy is configured per subgraph in a `type_superclasses` map.

Every identifier is normalised to a **CURIE** using the Bioregistry.
See the Identifiers & CURIEs section for details.

```json
{
  "grebi:nodeId": "hgnc:1100",
  "grebi:type": ["biolink:Gene"],
  "grebi:name": "BRCA1",
  "sourceIds": ["hgnc:1100", "ncbigene:672", "ensembl:ENSG00000012048"]
}
```

## Running queries

<query-template id="disease_to_genes" graph="dismech" disease_id="mondo:0005002" />

## Using the GrEBI API

The GrEBI REST API is served by a Spring Boot application on port **8090**.

```
http://localhost:8090/api
```

### Endpoints

#### Health check

```bash
curl http://localhost:8090/api/health
```

#### List available graphs

<api-example method="GET" url="/api/v1/graphs" />

#### List query templates for a graph

<api-example method="GET" url="/api/v1/graphs/dismech/query_templates" />

#### Execute a query template

<query-template id="disease_to_genes" graph="dismech" disease_id="mondo:0005002" />

#### Search nodes

<api-example method="GET" url="/api/v1/graphs/dismech/search" q="BRCA1" size="5" />

#### Get a specific node

<api-example method="GET" url="/api/v1/graphs/dismech/nodes/hgnc:1100" />

### Response format

All paginated endpoints return:

```json
{
  "content": [ ... ],
  "totalElements": 42,
  "totalPages": 5,
  "number": 0,
  "numberOfElements": 10
}
```

| Field | Type | Description |
|-------|------|-------------|
| `content` | array | The result rows for the current page |
| `totalElements` | integer | Total number of matching results |
| `totalPages` | integer | Number of pages at the current page size |
| `number` | integer | Zero-based current page number |
| `numberOfElements` | integer | Number of rows in this page |

### Error handling

Errors return standard HTTP status codes with a JSON body:

```json
{
  "status": 404,
  "error": "Not Found",
  "message": "Graph 'foo' not found"
}
```

## Using the GrEBI MCP server
