use grebi_golden::GoldenCase;

/// The merged entities of the clique-merge test subgraph, as the pipeline recorded them.
#[test]
fn merges_the_assigned_nodes_into_one_entity_per_group() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_merge"), "tests/golden/test_clique_merge")
        .args(["--exclude-props", "ols:hierarchicalProperty,ols:synonymProperty,ols:curie,ols:shortForm,ols:ontologyPreferredPrefix,ols:iri,ols:uri,ols:imported,ols:hasHierarchicalParents,ols:hasHierarchicalChildren,ols:hasDirectParents,ols:hasDirectChildren,ols:numDescendants,ols:numHierarchicalDescendants,oboinowl:id,oboinowl:url,monarch:iri,cco:hasDocument,cco:hasMolecule", "--prioritise-datasources", "Ontologies.biolink,Ontologies.ro,Ontologies.chebi,Ontologies.hp,Ontologies.mp,Ontologies.mondo,Ontologies.oba,Ontologies.efo,Ontologies.doid,HGNC,IMPC", "--annotate-subgraph-name", "test_clique_merge", "TestCliqueMerge:$CASE/TestCliqueMerge_0c1b5f74_nodes_with_ids.sorted.jsonl.gz", "TestCliqueMerge:$CASE/TestCliqueMerge_6d67346f_nodes_with_ids.sorted.jsonl.gz"]).stdout("merged.jsonl")
        .run();
}
