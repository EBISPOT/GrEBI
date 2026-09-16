use grebi_golden::GoldenCase;

/// Each node gets the id of its equivalence group and keeps its source ids.
#[test]
fn assigns_group_ids_to_the_ingested_nodes() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_assign_ids"), "tests/golden/test_clique_merge")
        .args(["--identifier-properties", "id,owl:equivalentClass,owl:equivalentProperty,owl:sameAs,grebi:hashId,grebi:equivalentTo,ols:iri,ols:shortForm,hgnc:ensembl_gene_id,obo:chebi/inchi,obo:chebi/inchikey,obo:chebi/smiles,otar:canonicalSmiles,otar:inchiKey,impc:pmId,impc:humanGeneAccId,monarch:iri,skos:exactMatch,ncit:P368,ncit:C98965,dcterms:identifier,oboinowl:hasAlternativeId,robokop:equivalent_identifiers,mesh.vocab:identifier", "--groups-txt", "$CASE/groups.txt"]).stdin("nodes.jsonl").stdout("output.jsonl")
        .run();
}
