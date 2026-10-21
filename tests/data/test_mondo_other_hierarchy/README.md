# test_mondo_other_hierarchy

A tiny MONDO (`mondo_tiny.ofn`) with one `MONDO:otherHierarchy` cross-reference
of each kind MONDO has, run through the production `MONDO.otherHierarchy`
ingest (owlmake's SPARQL over the OWL, then `grebi_tsv2jsonl`), alongside
stand-in nodes (`reference_terms.jsonl`) for every term the xrefs name.

Expected: anemia (`mondo:0002280`) and Anemia (`hp:0001903`) become one node
with both ids, since that xref is a one-to-one HP pair on a live class. Every
other term stays its own node: the obsolete classes' xrefs, the `consider`
annotation, the NCIT and HGNC targets, glutaric aciduria's two HP targets, and
the HP term two osteomyelitis diseases share are all left out of the ingest.
