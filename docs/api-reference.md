# API Reference

The GrEBI REST API is served by a Spring Boot application on port **8090**.

## Base URL

```
http://localhost:8090/api
```

## Endpoints

### Health check

```bash
curl http://localhost:8090/api/health
```

### List available graphs

<api-example method="GET" url="/api/v1/graphs" />

### List query templates for a graph

<api-example method="GET" url="/api/v1/graphs/dismech/query_templates" />

### Execute a query template

<query-template id="disease_to_genes" graph="dismech" params='{"disease_id":"mondo:0005002"}' />

### Search nodes

<api-example method="GET" url="/api/v1/graphs/dismech/search" params='{"q":"BRCA1","size":"5"}' />

### Get a specific node

<api-example method="GET" url="/api/v1/graphs/dismech/nodes/hgnc:1100" />

## Response format

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

## Error handling

Errors return standard HTTP status codes with a JSON body:

```json
{
  "status": 404,
  "error": "Not Found",
  "message": "Graph 'foo' not found"
}
```

See also: [Query Templates](./query-templates.md) for template-specific usage.
