
# Using GrEBI

There are four main ways to use GrEBI. First, you can visually [navigate the graph](#navigating-the-graph) and [perform queries](#running-queries) using the GrEBI user interface. Secondly, GrEBI provides a [REST API](#using-the-grebi-api) for use from scripts (we have tested Python and R, but any language with a HTTP client will work). Thirdly, GrEBI can be used as an [MCP server](#using-the-grebi-mcp-server) to enable LLM agents e.g. Claude, OpenAI to perform queries on your behalf. Finally, we provide [materialised tables](#materialised-tables) of a selection of large and expensive graph queries as CSV exports on the EBI FTP, so that the results can be interpreted using simple tabular tools.

## Navigating the graph

GrEBI is designed for graphs which have very large numbers of edges (> 1 billion). In order to accomplish this without crashing your computer, GrEBI provides a controlled interactive interface in which you can _path through_ the graph without pulling in all of the nodes and edges at once. Starting at a given node (for example, a disease, drug, or phenotype you are interested in), GrEBI allows you to follow edges - in either direction - and leave a trail of virtual breadcrumbs each time you traverse to the next node, creating a path. 

<todo>Add example pathing through the graph</code>

## Running queries

<query-template id="disease_to_genes" graph="dismech" disease_id="mondo:0005002" />

## Using the GrEBI API

### Endpoints

#### List available graphs

<api-example method="GET" url="/api/v1/graphs" />

#### List query templates for a graph

<api-example method="GET" url="/api/v1/graphs/dismech/query_templates" />

#### Execute a query template

<query-template id="disease_to_genes" graph="dismech" disease_id="mondo:0005002" />

#### Search nodes

<api-example method="GET" url="/api/v1/graphs/dismech/search" q="BRCA1" size="5" />

The text matches names by containment, best similarity first. Add `exactMatch=true` to match the whole name instead, ignoring case, for example to resolve a list of symbols one by one. The `grebi:type` and `grebi:datasources` parameters narrow the results, and the MCP `search_nodes` tool takes the same options. The same search at `/search.csv` streams every result (up to 100,000) as a CSV file, and a node's `/incoming_edges.csv` and `/outgoing_edges.csv` do the same for its edges, with the edge's other properties as JSON in the last column.

Graphs with embedding models also have `/semantic_search?q=…&model=…`, which ranks nodes by the similarity of their embeddings to the text. With `page` and `size` it returns a page like the node search, with the type and datasource counts of the nearest candidates as facets and the same filters applied inside the ranking; with `n` alone it returns a plain list of the nearest `n`.

#### Get a specific node

<api-example method="GET" url="/api/v1/graphs/dismech/nodes/hgnc:1100" />

A node whose labels exist in several languages lists them in `grebi:languages`. Its values are in English unless they carry a language: a translation is a reified value such as `{"grebi:value": "Gène A", "grebi:properties": {"grebi:lang": ["fr"]}}`. Ask for one language with `?lang=fr` on this endpoint, on the node search or on the semantic search: that language's values come first on every property, English follows as the fallback, and other languages are left out. The MCP `get_node` and `search_nodes` tools take the same `lang` argument.

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

GrEBI exposes a Streamable HTTP MCP endpoint at `/api/v1/mcp`.

The MCP server makes query templates available as tools, so LLM agents can execute the same pre-baked graph queries that are available in the browser and REST API. It also provides a small graph-traversal toolset for exploring the graph directly:

- `search_nodes` to find candidate starting nodes
- `get_node` to inspect a specific node
- `get_node_edge_counts` to summarise incoming and outgoing edges by type and datasource
- `list_node_edges` to traverse incoming or outgoing edges, with an optional lightweight `refsOnly` mode
- `get_edge` to inspect a specific edge

The MCP server also publishes read-only resources for available graphs, query topics, query templates, and graph statistics.
