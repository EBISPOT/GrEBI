use grebi_golden::GoldenCase;

/// The equivalence groups of the clique-merge test subgraph, as the pipeline recorded them.
#[test]
fn groups_equivalent_identifiers() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_identifiers2groups"), "tests/golden/test_clique_merge")
        .args(["--add-group", "grebi:name,ols:label,rdfs:label,monarch:name,impc:name,reactome:displayName,dcterms:title,ncit:Preferred_Name,robokop:name,otar:name", "--add-group", "grebi:description,iao:definition,iao:0000115,monarch:description,ols:definition,robokop:description,otar:description", "--add-group", "grebi:synonym,monarch:synonym,iao:alternative_label,ols:synonym,oboinowl:hasExactSynonym,dcterms:alternative,otar:synonyms", "--add-group", "mondo:0000001,ogms:0000031", "--add-group", "biolink:broad_match,skos:broader,skos:broadMatch,ols:directAncestor", "--add-group", "biolink:subclass_of,ols:directParent,rdfs:subClassOf,rdfs:subPropertyOf", "--add-group", "rdfs:isDefinedBy,ols:ontologyIri,ols:ontologyId", "--add-group", "biolink:gene_to_disease_association,ncit:Gene_Associated_With_Disease", "--add-group", "biolink:disease_has_location,efo:has_disease_location"]).stdin("identifiers.tsv").stdout("groups.txt")
        .run();
}
