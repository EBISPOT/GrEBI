# Graph Queries

GrEBI supports two styles of querying:

1. **Query templates** — pre-built parameterised Cypher queries (recommended)
2. **Direct Cypher** — via the Neo4j Browser or Bolt protocol

## Using query templates

Query templates are the easiest way to get started. Each template defines a
Cypher query with named parameters that you fill in.

Browse all templates on the **Queries** page in the UI, or list them via the
API:

<api-example method="GET" url="/api/v1/graphs/dismech/query_templates" />

For a full walkthrough, see [Query Templates](./query-templates.md).

## Direct Cypher via Neo4j Browser

If you need more flexibility, you can connect to the Neo4j Browser at
<http://localhost:7474> and execute arbitrary Cypher:

```cypher
MATCH (g:`biolink:Gene`)-[:sourceId]->(:Id { id: "hgnc:1100" })
RETURN g
LIMIT 10
```

> **Tip:** Use `LIMIT` to avoid accidentally returning the entire graph.

## Example: finding gene–disease associations

This query finds all diseases associated with the gene `hgnc:1100` (BRCA1):

```cypher
MATCH (gene:`biolink:Gene`)-[:sourceId]->(:Id { id: "hgnc:1100" })
MATCH (gene)-[assoc:`biolink:related_to`]->(disease:`biolink:Disease`)
RETURN disease.`grebi:name` AS disease_name,
       disease.`grebi:nodeId` AS disease_id
ORDER BY disease_name
LIMIT 20
```

## Materialised queries

Some expensive queries are pre-computed and stored as **materialised tables**.
Access them from the **Tables** page in the UI or via the API:

<api-example method="GET" url="/api/v1/graphs/dismech/query_templates" />

## Performance tips

| Tip | Why |
|-----|-----|
| Always anchor on an indexed property (`:Id { id: ... }`) | Avoids full graph scans |
| Use `LIMIT` | GrEBI graphs can have 100M+ nodes |
| Prefer query templates over ad-hoc Cypher | Templates are optimised and tested |
