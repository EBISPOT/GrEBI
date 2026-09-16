# test_primekg data

A small **real** subset of PrimeKG's `kg.csv`, used by the `test_primekg`
subgraph to exercise `dataload/01_ingest/primekg.py` (GitHub issue #58:
PrimeKG stores MONDO, HPO, GO and UBERON ids without their leading zeros, and
its CTD exposure ids are MeSH UIs under a source name that is not a prefix).

## Provenance

`kg.csv` is the header plus eight rows copied verbatim from
<https://dataverse.harvard.edu/api/access/datafile/6180620> (PrimeKG kg.csv,
8,100,498 rows), chosen around psoriasis (MONDO `5083`, i.e. MONDO:0005083):

| relation | from | to | Exercises |
| --- | --- | --- | --- |
| `indication` | Allantoin (DrugBank DB11100) | psoriasis (MONDO 5083) | drug -> disease edge onto the padded id |
| `contraindication` | Lithium citrate (DrugBank DB14507) | psoriasis | same, different relation |
| `disease_protein` | APOE (NCBI 348) | psoriasis | NCBI becomes NCBIGene |
| `disease_disease` | genetic skin disease (MONDO 24255) | psoriasis | both ends padded |
| `disease_phenotype_positive` | a disease (MONDO) | a phenotype (HPO) | HPO padding |
| `bioprocess_protein` | A1BG (NCBI 1) | neutrophil degranulation (GO 43312) | GO padding |
| `anatomy_protein_present` | TSPAN6 (NCBI 7105) | uterine cervix (UBERON 2) | UBERON padding, a one-digit id |
| `exposure_protein` | Agent Orange (CTD D000075182) | CYP1A1 (NCBI 1543) | CTD exposure ids are MeSH UIs and become `mesh:` |

`reference_terms.jsonl` holds stand-ins for the ontology terms those rows
reference, with their padded ids (`mondo:0005083`, `hp:...`, `go:0043312`,
`uberon:0000002`, ...) and the MeSH descriptor `mesh:D000075182`. In the built graph each PrimeKG node and its ontology
term must be one node carrying both the `PrimeKG` and `OntologyTerms`
datasources, with the PrimeKG edges ending on it.

## Regenerating / extending

`grep` rows out of the full `kg.csv` (it is plain CSV; names can contain
quoted commas) and add any newly referenced ontology terms to
`reference_terms.jsonl`.
