//! Golden tests pinning what each Python ingest script produces today, run the
//! same way the pipeline runs them. When a script is ported to Rust its case
//! moves to the new crate and only the command changes.
use grebi_golden::GoldenCase;

fn script(path: &str) -> String {
    format!("{}/../../{}", env!("CARGO_MANIFEST_DIR"), path)
}

fn python(script_path: &str, case: &str) -> GoldenCase {
    GoldenCase::new("python3", format!("tests/golden/{}", case))
        .arg(script(script_path))
        .env("GREBI_DATALOAD_HOME", format!("{}/../..", env!("CARGO_MANIFEST_DIR")))
}


/// A BioStudies FTP tree walked for accession-level PageTab JSON files.
#[test]
fn biostudies() {
    python("01_ingest/biostudies.py", "biostudies")
        .env("GREBI_DATASOURCE_ID", "BioStudies").env("GREBI_INGEST_DATASOURCE_NAME", "BioStudies")
        .args(["--", "$CASE/fire"]).stdout("output.jsonl").run();
}

/// The PRIDE projects export, one node per project.
#[test]
fn pride() {
    python("01_ingest/pride.py", "pride")
        .env("GREBI_DATASOURCE_ID", "PRIDE").env("GREBI_INGEST_DATASOURCE_NAME", "PRIDE")
        .args(["--", "$CASE/projects.json"]).stdout("output.jsonl").run();
}

/// An Expression Atlas experiment: the gene-level TPM table with its
/// configuration and condensed SDRF alongside, medians above the cutoff only.
#[test]
fn expression_atlas_e_mtab_513() {
    python("01_ingest/expression_atlas.py", "expression_atlas")
        .args(["--min-median-tpm", "0.5", "--", "$CASE/E-MTAB-513/E-MTAB-513-tpms.tsv"])
        .stdout("E-MTAB-513.jsonl").run();
}

#[test]
fn expression_atlas_e_test_1() {
    python("01_ingest/expression_atlas.py", "expression_atlas")
        .args(["--min-median-tpm", "0.5", "--", "$CASE/E-TEST-1/E-TEST-1-tpms.tsv"])
        .stdout("E-TEST-1.jsonl").run();
}





/// The three pesticide registers, read from spreadsheets.
#[test]
fn hett_pesticides_eu() {
    python("01_ingest/hett_pesticides_eu.py", "hett_pesticides_eu")
        .args(["--datasource-name", "HETT_Pesticides.EU"]).stdin("input.xlsx").stdout("output.jsonl").run();
}

#[test]
fn hett_pesticides_gb() {
    python("01_ingest/hett_pesticides_gb.py", "hett_pesticides_gb")
        .args(["--datasource-name", "HETT_Pesticides.GB"]).stdin("input.xlsx").stdout("output.jsonl").run();
}

#[test]
fn hett_pesticides_appril() {
    python("01_ingest/hett_pesticides_appril.py", "hett_pesticides_appril")
        .args(["--datasource-name", "HETT_Pesticides.APPRIL"]).stdin("input.xlsx").stdout("output.jsonl").run();
}
