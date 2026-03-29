# Query Templates

Query templates are parameterised Cypher queries stored as YAML files in
`query_templates/`. They are the primary way to query GrEBI.

## Anatomy of a template

```yaml
title: Genes related to a disease
question: What [genes]{gene} are related to {disease_id}?
graphs:
  - dismech
topics:
  - genetics
cypher_match_fragment: |-
  MATCH (disease:`biolink:Disease`)-[:sourceId]->(:Id { id: $disease_id })
  MATCH (gene:`biolink:Gene`)-[assoc:`biolink:related_to`]->(disease)
cypher_return_fragment: |-
  RETURN DISTINCT gene {...}, disease {...}, assoc.edge_id
params:
  - param_id: disease_id
    param_type: SourceId
result_columns:
  - column_id: gene
    column_type: GraphNodeId
examples:
  - title: chronic obstructive pulmonary disease
    params:
      disease_id: "mondo:0005002"
```

## Running a template via the API

<query-template id="disease_to_genes" graph="dismech" params='{"disease_id":"mondo:0005002"}' />

## Running a template via the UI

Navigate to **Queries → disease_to_genes**, fill in the parameters, and click
**Run**. Results are displayed in a table and can be downloaded as CSV.

## Writing your own template

1. Create a new YAML file in `query_templates/`, for example
   `my_query.yaml`.
2. Add at least one **example** with parameters so the integration tests
   can validate it.
3. List the **graphs** the query applies to.
4. Restart the API (or redeploy) to pick up the new template.

See the [API Reference](./api-reference.md) for the full endpoint schema.

## Live example

<query-template id="disease_to_genes" graph="dismech" params='{"disease_id":"mondo:0005002"}' />
