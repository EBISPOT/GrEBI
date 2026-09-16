use grebi_golden::GoldenCase;

/// Identifier values of the ingested edge subjects, as the pipeline recorded them.
#[test]
fn extracts_the_identifier_properties_of_the_edge_subjects() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_extract_identifiers"), "tests/golden/test_clique_merge")
        .args(["--identifier-properties", "id,owl:equivalentClass,owl:equivalentProperty,owl:sameAs,grebi:hashId,grebi:equivalentTo,ols:iri,ols:shortForm,hgnc:ensembl_gene_id,obo:chebi/inchi,obo:chebi/inchikey,obo:chebi/smiles,otar:canonicalSmiles,otar:inchiKey,impc:pmId,impc:humanGeneAccId,monarch:iri,skos:exactMatch,ncit:P368,ncit:C98965,dcterms:identifier,oboinowl:hasAlternativeId,robokop:equivalent_identifiers,mesh.vocab:identifier"]).stdin("edges_nodes.jsonl").stdout("edges_identifiers.tsv")
        .run();
}

/// Identifier values of the ingested nodes, as the pipeline recorded them.
#[test]
fn extracts_the_identifier_properties_of_the_nodes() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_extract_identifiers"), "tests/golden/test_clique_merge")
        .args(["--identifier-properties", "id,owl:equivalentClass,owl:equivalentProperty,owl:sameAs,grebi:hashId,grebi:equivalentTo,ols:iri,ols:shortForm,hgnc:ensembl_gene_id,obo:chebi/inchi,obo:chebi/inchikey,obo:chebi/smiles,otar:canonicalSmiles,otar:inchiKey,impc:pmId,impc:humanGeneAccId,monarch:iri,skos:exactMatch,ncit:P368,ncit:C98965,dcterms:identifier,oboinowl:hasAlternativeId,robokop:equivalent_identifiers,mesh.vocab:identifier"]).stdin("nodes.jsonl").stdout("nodes_identifiers.tsv")
        .run();
}
