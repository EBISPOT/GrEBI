# Data Model

GrEBI builds a single unified graph from many upstream knowledge graphs.

## Nodes — Cliques of equivalent entities

Each node in GrEBI represents a **clique**: a group of identifiers that have
been determined to refer to the same real-world entity. For example, the gene
*BRCA1* may appear as `hgnc:1100`, `ncbigene:672`, and `ensembl:ENSG00000012048`.
GrEBI merges these into a single node with one **canonical ID** and links to all
source IDs.

## Edges — Relationships between cliques

Edges connect two clique nodes. Each edge carries:

| Property | Description |
|----------|-------------|
| `edge_id` | Unique identifier for this edge |
| Type label | A Biolink predicate, e.g. `biolink:related_to` |
| Provenance | The datasource(s) that asserted the relationship |

## Type hierarchy

Nodes are labelled with one or more **Biolink types** such as
`biolink:Gene`, `biolink:Disease`, or `biolink:PhenotypicFeature`.
The type hierarchy is configured per subgraph in a `type_superclasses` map.

## Identifiers

Every identifier is normalised to a **CURIE** using the Bioregistry.
See [Identifiers & CURIEs](./identifiers.md) for details.

## Example node (JSON)

```json
{
  "grebi:nodeId": "hgnc:1100",
  "grebi:type": ["biolink:Gene"],
  "grebi:name": "BRCA1",
  "sourceIds": ["hgnc:1100", "ncbigene:672", "ensembl:ENSG00000012048"]
}
```
