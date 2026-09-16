# PRIDE metadata fixture

`projects.json` is a deliberately reduced selection of fields from public
project `PXD001357` in the [PRIDE bulk export](https://www.ebi.ac.uk/pride/ws/archive/v3/projects/all),
downloaded on 2026-09-15. It keeps the project DOI, dates, one organism, one
instrument, a publication and one related project (under both of its IDs).
No experimental files are needed or downloaded.

`reference_nodes.json` supplies minimal test entities, not additional source
exports. The E2E test exercises ProteomeXchange alias merging, NEWT taxonomy
normalisation, ontology and publication links, and duplicate related-project
references. Unit tests separately check excluded fields and malformed exports.
