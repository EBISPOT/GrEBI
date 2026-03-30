# Identifiers & CURIEs

GrEBI normalises every identifier to a **CURIE** (Compact URI) using the
[Bioregistry](https://bioregistry.io).

## Format

A CURIE has two parts separated by a colon:

```
prefix:localId
```

For example:

| CURIE | Meaning |
|-------|---------|
| `mondo:0005002` | COPD in the MONDO ontology |
| `hgnc:1100` | BRCA1 in the HGNC gene register |
| `hp:0001234` | A phenotype in the Human Phenotype Ontology |
| `chebi:15365` | Aspirin in ChEBI |

> **Note:** GrEBI uses **lowercase prefixes** (`hgnc`, not `HGNC`).

## Why CURIEs?

Different datasources use different schemes for the same entity:

- UniProt may reference `HGNC:1100`
- MONARCH may reference `NCBIGene:672`
- OLS may reference `ensembl:ENSG00000012048`

The GrEBI pipeline maps all of these to their **canonical CURIE** form and
groups them into a single clique. See [Data Model](./data-model.md) for how
cliques work.

## Prefix service

The GrEBI stack includes a **prefix service** that resolves CURIEs to full
IRIs and vice-versa:

```bash
curl "http://localhost:8082/curie_to_iri?curie=hgnc:1100"
```

```json
{"iri": "https://identifiers.org/hgnc:1100"}
```
