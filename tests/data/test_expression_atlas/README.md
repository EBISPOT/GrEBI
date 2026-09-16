# Expression Atlas fixture

Retrieved 2026-09-16 from the Expression Atlas FTP tree:
https://ftp.ebi.ac.uk/pub/databases/microarray/data/atlas/experiments/

- `E-MTAB-513`: first five gene rows, liver and lung columns only. Configuration
  and condensed SDRF retain exactly the assays for those two groups.
- `E-MTAB-4045`: first gene and three subsequent genes with median TPM > 0.5 in
  either selected group. Groups are cotyledon-stage embryo proper and suspensor;
  the XML and SDRF retain only their assays. Values and identifiers are unchanged.
- `E-TEST-1`: explicitly synthetic. Tests a five-number median exactly at the
  cutoff, high maxima that must not substitute for the median, disease/cell-line
  exclusion, multiple developmental contexts for one anatomy, and cross-study
  merging with the real human fixture.
- `reference_nodes.json`: minimal existing entities to exercise gene/anatomy
  merging and taxon links. Plant anatomy nodes come from the ingest itself.
- `experiments.json`: a small synthetic catalogue listing the three studies
  above and a differential study with no files. The E2E download uses the same
  catalogue discovery as production; the differential entry must be ignored.
- `E-STALE-1`: deliberately invalid cached TPM file staged by the reference
  datasource but absent from the catalogue. The manifest must exclude it from
  ingestion even though it matches the input glob.

No individual-cell matrices, raw reads, or linked experimental files are used.
