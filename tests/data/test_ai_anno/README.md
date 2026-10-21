# test_ai_anno

A manifest (`latest.txt`) of five small annotation tables in ai_anno's
published layout (`<project>/<release>/<table>_annotations.tsv`, plain rather
than gzipped so they are diffable), run through the production `AI_anno`
datasource: the catalogue generator turns the manifest into the download
list, and the generic tabular-edge pipeline (`grebi_tsv2jsonl`,
`grebi_transform_jsonl`, `grebi_nodes2edges`) reads each table.
`reference_terms.jsonl` holds stand-ins for the records, with the identifiers
their own ingests give them (the MetaboLights study's
`metabolights:MTBLS1`), and for the terms the mappings name, so the expected
output shows the annotations landing as edges.

The tables' ids are Bioregistry CURIEs (`biostudies:E-AFMX-5`,
`pride.project:PXD000001`, `metabolights:MTBLS1`, `pubmed:29121268`) and the
GWAS Catalog's bare accession (`GCST000187`), as ai_anno writes them; the
publication table's rows carry `record_type` `biolink:Publication`.

Expected: each mapped item is an `ai_anno:<column>` edge (`ai_anno:diseases`,
`ai_anno:exposures`, ...) from the record to the term, carrying the row's
other cells (`ai_anno:value`, `ai_anno:mapping_label`,
`ai_anno:mapping_reason`, `ai_anno:extraction_model`) and the project as edge
properties: BioStudies E-AFMX-5 to two MONDO diseases, PRIDE PXD000001 to its
organism, MetaboLights MTBLS1 to its organism and an ECTO exposure, GWAS
Catalog GCST000187 to a COHO cohort, and the publication pubmed:29121268
(which GCST000187 cites through gwas:pubmedid), typed `biolink:Publication`,
to an exposure. The unmapped items (S-BSST2's disease, PXD000001's exposure,
GCST000187's second cohort) leave no trace, and no table makes a node a
stand-in does not already have, apart from the publication.
