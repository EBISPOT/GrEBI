# test_reactome data

A small **real** subset of the Reactome graph dump, used by the `test_reactome`
subgraph to exercise `grebi_ingest_reactome`'s identifier handling (GitHub
issue #26: Reactome diseases were not clique-merged with the ontology diseases).

## Provenance

`reactome.jsonl` holds five lines copied verbatim from the APOC JSON export of
the Reactome graph database that the production datasource ingests
(<https://ftp.ebi.ac.uk/pub/databases/spot/kg/data/reactome.json.gz>,
2026-03-21 build; see `dataload/reactome/export_reactome_dump_to_json.sh`):

| Line | Object | Exercises |
| --- | --- | --- |
| node `974984` | `Disease` "psoriasis": `databaseName` DOID, `identifier` 8893, OLS search URL | the DOID id must come from `databaseName:identifier`, because the URL does not compact |
| node `974977` | `ProteinDrug` etanercept, `stId` R-ALL-9714949 | stable-id equivalence |
| node `2254953` | `ChemicalDrug` acitretin, `stId` R-ALL-9009800 | stable-id equivalence |
| 2 relationships | `disease` from each drug to the Disease | drug -> disease edges must end on the merged ontology node |

`reference_terms.jsonl` is a stand-in for the OLS datasources: the DOID term
`doid:8893` and the MONDO term `mondo:0005083`, which declares `skos:exactMatch`
to the DOID term. In the built graph all three, the Reactome Disease object
and both ontology terms, must be one node carrying the `Reactome` datasource
and both drugs' `reactome:disease` edges.

## Regenerating / extending

`zcat reactome.json.gz | grep -F '"974984"'` finds the disease and every line
that references it; node lines for other ids are found the same way. Keep the
lines verbatim (they are the ingester's input format).
