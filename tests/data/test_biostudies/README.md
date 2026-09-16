# test_biostudies data

The PageTab JSON fixture follows the accession-level metadata published for
`S-BSST1` in the [BioStudies FTP tree](https://ftp.ebi.ac.uk/biostudies/fire/S-BSST/001/S-BSST1/S-BSST1.json).
It retains representative study metadata, a linked ENA project, and one file
entry so the test can verify that submission files are not emitted by the
ingest. `reference_nodes.jsonl` supplies the linked ENA study in the isolated
test graph.
