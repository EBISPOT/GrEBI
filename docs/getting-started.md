# Getting Started

This guide walks you through running your first GrEBI query.

## Prerequisites

You need a running GrEBI stack. The easiest way is Docker Compose:

```bash
cd webapp
docker-compose up -d
```

Once the stack is healthy you can access:

| Service | URL |
|---------|-----|
| UI | <http://localhost:3000> |
| API | <http://localhost:8090> |
| Neo4j Browser | <http://localhost:7474> |

## Your first query

Pick a graph (e.g. `dismech`) and run a query template from the command line:

```bash
curl -s "http://localhost:8090/api/v1/graphs/dismech/query/disease_to_genes?disease_id=mondo:0005002" | jq .
```

The response is a paginated JSON object containing matching rows.
See the [API Reference](./api-reference.md) for the full schema.

## Next steps

- Browse the available [Query Templates](./query-templates.md)
- Learn about [Identifiers & CURIEs](./identifiers.md) used throughout GrEBI
- Understand the [Data Model](./data-model.md) — nodes, edges, and cliques
