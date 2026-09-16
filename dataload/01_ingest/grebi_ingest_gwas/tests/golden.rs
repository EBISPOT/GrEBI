use grebi_golden::GoldenCase;

/// The associations file of the GWAS test subgraph: SNP nodes with their
/// associated traits, mapped genes and study accessions.
#[test]
fn ingests_associations() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_ingest_gwas"), "tests/golden/test_gwas")
        .args(["--filename", "$CASE/gwas-catalog-download-associations-alt-full.tsv"])
        .stdin("gwas-catalog-download-associations-alt-full.tsv")
        .stdout("associations.jsonl")
        .run();
}

/// The studies file of the GWAS test subgraph: study nodes with their traits.
#[test]
fn ingests_studies() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_ingest_gwas"), "tests/golden/test_gwas")
        .args(["--filename", "$CASE/gwas-catalog-studies-download-alternative-v1.0.2.1.txt"])
        .stdin("gwas-catalog-studies-download-alternative-v1.0.2.1.txt")
        .stdout("studies.jsonl")
        .run();
}
