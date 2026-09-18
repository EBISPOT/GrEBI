
# MCP server

GrEBI is also a [Model Context Protocol](https://modelcontextprotocol.io) (MCP) server, so LLM agents can work with the knowledge graphs through tools rather than raw HTTP: the same query templates the browser and the REST API offer, and a small set of tools for searching, looking up and reading nodes and following their edges. Everything is read-only.

## Connecting

The endpoint is `/api/v1/mcp` on the same host as the API, over the streamable HTTP transport, without authentication. For the EMBL-EBI instance a client configuration looks like this:

```json
{
  "mcpServers": {
    "grebi": {
      "url": "https://www.ebi.ac.uk/spot/kg/api/v1/mcp"
    }
  }
}
```

The server publishes its tools and resources on connection, as any MCP client expects. What it publishes is also available as plain JSON at `/api/v1/mcp/catalogue`, which is what the reference below is rendered from.

## Graphs and their queries

An instance serves several graphs, and each graph has its own query templates: the questions its datasources can answer. The tools an agent can call therefore depend on the graph it is working with, and every tool takes the graph as its first argument, `graph`. The fixed tools work on every graph; the template tools are listed below under each graph they are for. The `grebi://graphs` resource lists the graphs of the instance, and `grebi://query_templates` its templates, so an agent can find out both at run time.

A template tool answers a page at a time: with the template's own parameters it takes `sortBy` and `sortDir` for the order, and `pageNum` and `pageSize` (at most 100) for the page, and returns `rows` with `totalNumRows`, `totalNumPages`, `pageNum` and `pageSize`. Tool calls share the API's limits: text arguments of at most 4,096 characters and 300 calls a minute from one client.

## What the server offers

<mcp-reference />
