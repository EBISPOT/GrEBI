# test_biostudies data

`biostudies.jsonl` is a one-study snapshot in the form the pipeline downloads
from the SPOT FTP area (one PageTab JSON document per line, see
`dataload/00_download/biostudies_snapshot.py`; the real one is gzipped, the
fixture is left uncompressed so it is diffable). The document follows the
accession-level metadata published for `S-BSST1` in the
[BioStudies FTP tree](https://ftp.ebi.ac.uk/biostudies/fire/S-BSST/001/S-BSST1/S-BSST1.json).
It retains representative study metadata, a linked ENA project, and one file
entry so the test can verify that submission files are not emitted by the
ingest. `reference_nodes.jsonl` supplies the linked ENA study in the isolated
test graph.
