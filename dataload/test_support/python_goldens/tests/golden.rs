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
