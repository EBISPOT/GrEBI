use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use arrow::datatypes::DataType;
use grebi_golden::GoldenCase;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const BIN: &str = env!("CARGO_BIN_EXE_grebi_make_download_tables");

/// The CSV: a column for a node's identifier and one for its name, lists
/// joined with semicolons, numbers as the rows spell them, no edge ids.
#[test]
fn the_rows_become_csv_with_the_columns_of_the_metadata() {
    GoldenCase::new(BIN, "tests/golden/gwas_by_gene")
        .args(["--in-metadata-json", "$CASE/gwas_by_gene.json", "--out-parquet-path", "gwas_by_gene.parquet"])
        .stdin("gwas_by_gene.results.jsonl")
        .stdout("gwas_by_gene.csv")
        .stderr("gwas_by_gene.stderr")
        .run();
}

/// No rows gives the header, so that the file still says what the table holds.
#[test]
fn no_rows_gives_the_header_alone() {
    GoldenCase::new(BIN, "tests/golden/empty")
        .args(["--in-metadata-json", "$CASE/gwas_by_gene.json", "--out-parquet-path", "empty.parquet"])
        .stdin("empty.results.jsonl")
        .stdout("empty.csv")
        .run();
}

/// The Parquet file holds the same rows, typed. It is compared as the JSON of
/// its rows, not byte for byte: the bytes name the version of the writer.
#[test]
fn the_parquet_file_holds_the_same_rows_typed() {
    let file = write_parquet("tests/golden/gwas_by_gene", "gwas_by_gene.results.jsonl");
    let reader = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&file).unwrap()).unwrap();

    let schema = reader.schema().clone();
    let described: Vec<String> = schema.fields().iter()
        .map(|f| format!("{} {}{}", f.name(), type_name(f.data_type()), if f.is_nullable() { "" } else { " not null" }))
        .collect();
    assert_eq!(described, [
        "gene_id string", "gene_label string", "snp_id string", "snp_label string",
        "trait_id string", "trait_label string", "study_accession string", "pubmed_id string",
        "p_value double", "or_or_beta double", "n_cases int64", "from_datasources list<string> not null",
    ]);

    let provenance: Vec<(String, Option<String>)> = reader.metadata().file_metadata().key_value_metadata().unwrap().iter()
        .filter(|kv| kv.key.starts_with("grebi:"))
        .map(|kv| (kv.key.clone(), kv.value.clone()))
        .collect();
    assert_eq!(provenance, [
        ("grebi:graph".to_string(), Some("g1".to_string())),
        ("grebi:query_id".to_string(), Some("gwas_by_gene".to_string())),
        ("grebi:title".to_string(), Some("GWAS associations of a gene".to_string())),
        ("grebi:description".to_string(), Some("Associations of the variants mapped to a gene".to_string())),
    ]);

    let mut rows = arrow_json::LineDelimitedWriter::new(Vec::new());
    for batch in reader.build().unwrap() {
        rows.write(&batch.unwrap()).unwrap();
    }
    rows.finish().unwrap();
    compare_with_recorded(Path::new("tests/golden/gwas_by_gene/gwas_by_gene.parquet.jsonl"), &rows.into_inner());
}

#[test]
fn no_rows_gives_a_parquet_file_with_the_schema_alone() {
    let file = write_parquet("tests/golden/empty", "empty.results.jsonl");
    let reader = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&file).unwrap()).unwrap();
    assert_eq!(reader.schema().fields().len(), 12);
    assert_eq!(reader.metadata().file_metadata().num_rows(), 0);
}

fn type_name(t: &DataType) -> String {
    match t {
        DataType::Utf8 => "string".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::List(item) => format!("list<{}>", type_name(item.data_type())),
        other => format!("{:?}", other),
    }
}

/// Runs the binary over a case's rows and returns the Parquet file it wrote.
fn write_parquet(case_dir: &str, rows: &str) -> PathBuf {
    let scratch = std::env::temp_dir().join(format!("grebi_download_tables_{}_{}", std::process::id(), rows));
    fs::create_dir_all(&scratch).unwrap();
    let file = scratch.join("out.parquet");
    let case_dir = fs::canonicalize(case_dir).unwrap();
    let output = Command::new(BIN)
        .arg("--in-metadata-json").arg(case_dir.join("gwas_by_gene.json"))
        .arg("--out-parquet-path").arg(&file)
        .stdin(Stdio::from(fs::File::open(case_dir.join(rows)).unwrap()))
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    file
}

fn compare_with_recorded(recorded: &Path, actual: &[u8]) {
    if std::env::var("UPDATE_GOLDEN").map(|v| v == "1").unwrap_or(false) {
        fs::write(recorded, actual).unwrap();
        return;
    }
    let expected = fs::read_to_string(recorded).unwrap_or_else(|e| panic!("{}: {}", recorded.display(), e));
    assert_eq!(std::str::from_utf8(actual).unwrap(), expected,
        "the rows of the Parquet file differ from {} (UPDATE_GOLDEN=1 rewrites it)", recorded.display());
}
