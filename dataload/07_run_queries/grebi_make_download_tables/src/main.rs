//! A materialised query's result rows, one JSON object per line on stdin, as
//! the table files people download from a release: CSV on stdout and Parquet
//! at --out-parquet-path, both written in the one pass over the rows.
//!
//! The columns are those of the query's metadata JSON (written by run_queries),
//! in the order of the template's result_columns, read from each row the way
//! the Postgres table writer reads them:
//!
//!   GraphNodeId    -> "<col>_id"     the node's identifier: not its node id but the
//!                                    identifier the column is about, a MONDO id for
//!                                    a disease (grebi_shared node_identifier)
//!                     "<col>_label"  its name; none if the node has no name
//!   DatasourceList -> "<col>"        list of strings; in CSV, joined with ";"
//!   float          -> "<col>"        double; in CSV, the number as the row spells it
//!   int            -> "<col>"        64 bit integer
//!   EdgeId         -> left out: edge ids mean nothing outside the graph
//!   anything else  -> "<col>"        string
//!
//! A value that is missing or null is an empty cell in CSV and a null in
//! Parquet, where every column but the lists is nullable. With no rows at all
//! the CSV is its header and the Parquet file its schema.
//!
//! The Parquet file says where it is from in its key-value metadata: grebi:graph,
//! grebi:query_id, grebi:title and grebi:description.

use std::fs::File;
use std::io::{self, BufRead, BufWriter};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Builder, Int64Builder, ListBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use clap::Parser;
use grebi_shared::query_results::{self, as_f64, as_text, flatten_to_strings, Col, ColKind};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::file::metadata::KeyValue;
use serde_json::Value;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    in_metadata_json: String,

    #[arg(long)]
    out_parquet_path: String,
}

/// Rows to an Arrow batch. Memory stays flat however many rows there are.
const BATCH_ROWS: usize = 50_000;

/// A number as the row spells it, if it is one: "1E-7" stays "1E-7".
fn number_as_written(v: Option<&Value>) -> Option<String> {
    as_f64(v)?;
    match v? {
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => Some(s.trim().to_string()),
        Value::Array(arr) => number_as_written(arr.first()),
        _ => None,
    }
}

enum Builder {
    Node { id: StringBuilder, label: StringBuilder },
    TextArray(ListBuilder<StringBuilder>),
    Float(Float64Builder),
    Int(Int64Builder),
    Text(StringBuilder),
}

fn list_of_strings() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
}

fn main() {
    let args = Args::parse();

    let metadata: Value = serde_json::from_reader(io::BufReader::new(
        File::open(&args.in_metadata_json).expect("Failed to open metadata json"),
    ))
    .expect("Failed to parse metadata json");

    let nid_prefix = query_results::node_id_prefix(&metadata);

    let cols: Vec<Col> = query_results::columns(&metadata)
        .into_iter()
        .filter(|c| c.column_type != "EdgeId" && c.column_type != "EdgeProps")
        .collect();

    let mut fields: Vec<Field> = Vec::new();
    let mut builders: Vec<Builder> = Vec::new();
    for col in &cols {
        match col.kind {
            ColKind::Node => {
                fields.push(Field::new(format!("{}_id", col.id), DataType::Utf8, true));
                fields.push(Field::new(format!("{}_label", col.id), DataType::Utf8, true));
                builders.push(Builder::Node { id: StringBuilder::new(), label: StringBuilder::new() });
            }
            ColKind::TextArray => {
                fields.push(Field::new(&col.id, list_of_strings(), false));
                builders.push(Builder::TextArray(ListBuilder::new(StringBuilder::new())));
            }
            ColKind::Float => {
                fields.push(Field::new(&col.id, DataType::Float64, true));
                builders.push(Builder::Float(Float64Builder::new()));
            }
            ColKind::Int => {
                fields.push(Field::new(&col.id, DataType::Int64, true));
                builders.push(Builder::Int(Int64Builder::new()));
            }
            ColKind::Text => {
                fields.push(Field::new(&col.id, DataType::Utf8, true));
                builders.push(Builder::Text(StringBuilder::new()));
            }
        }
    }
    let schema = Arc::new(Schema::new(fields));

    let mut csv_out = csv::Writer::from_writer(BufWriter::with_capacity(1024 * 1024 * 8, io::stdout().lock()));
    csv_out.write_record(schema.fields().iter().map(|f| f.name())).unwrap();

    let mut provenance: Vec<KeyValue> = Vec::new();
    for (key, field) in [("grebi:graph", "subgraph"), ("grebi:query_id", "id"), ("grebi:title", "title"), ("grebi:description", "description")] {
        if let Some(value) = metadata.get(field).and_then(|v| v.as_str()) {
            provenance.push(KeyValue::new(key.to_string(), value.to_string()));
        }
    }
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(9).unwrap()))
        .set_key_value_metadata(Some(provenance))
        .build();
    let mut parquet_out = ArrowWriter::try_new(
        File::create(&args.out_parquet_path).expect("Failed to create the parquet file"),
        schema.clone(),
        Some(properties),
    )
    .unwrap();

    let mut cells: Vec<String> = Vec::with_capacity(schema.fields().len());
    let mut n_rows: usize = 0;
    let mut n_in_batch: usize = 0;
    let mut line_number: usize = 0;

    for line in io::stdin().lock().lines() {
        let line = line.unwrap();
        line_number += 1;
        if line.trim().is_empty() {
            continue;
        }
        let row: serde_json::Map<String, Value> = match serde_json::from_str(&line) {
            Ok(row) => row,
            Err(e) => {
                eprintln!("line {} is not a JSON object: {}", line_number, e);
                std::process::exit(1);
            }
        };

        cells.clear();
        for (col, builder) in cols.iter().zip(builders.iter_mut()) {
            let v = row.get(&col.id).filter(|v| !v.is_null());
            match builder {
                Builder::Node { id, label } => {
                    let node_id = query_results::node_identifier(v, &col.id_prefixes, &nid_prefix);
                    let node_label = query_results::node_name(v);
                    id.append_option(node_id.as_deref());
                    label.append_option(node_label.as_deref());
                    cells.push(node_id.unwrap_or_default());
                    cells.push(node_label.unwrap_or_default());
                }
                Builder::TextArray(list) => {
                    let values = v.map(flatten_to_strings).unwrap_or_default();
                    for value in &values {
                        list.values().append_value(value);
                    }
                    list.append(true);
                    cells.push(values.join(";"));
                }
                Builder::Float(floats) => {
                    floats.append_option(as_f64(v));
                    cells.push(number_as_written(v).unwrap_or_default());
                }
                Builder::Int(ints) => {
                    let value = as_f64(v).map(|f| f as i64);
                    ints.append_option(value);
                    cells.push(value.map(|i| i.to_string()).unwrap_or_default());
                }
                Builder::Text(text) => {
                    let value = as_text(v);
                    text.append_option(value.as_deref());
                    cells.push(value.unwrap_or_default());
                }
            }
        }
        csv_out.write_record(&cells).unwrap();

        n_rows += 1;
        n_in_batch += 1;
        if n_in_batch == BATCH_ROWS {
            write_batch(&mut parquet_out, &schema, &mut builders);
            n_in_batch = 0;
        }
    }
    if n_in_batch > 0 {
        write_batch(&mut parquet_out, &schema, &mut builders);
    }

    csv_out.flush().unwrap();
    parquet_out.close().unwrap();

    eprintln!(
        "grebi_make_download_tables ({}): wrote {} rows, {} columns",
        metadata.get("id").and_then(|v| v.as_str()).unwrap_or("?"),
        n_rows,
        schema.fields().len()
    );
}

fn write_batch(out: &mut ArrowWriter<File>, schema: &Arc<Schema>, builders: &mut [Builder]) {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for builder in builders.iter_mut() {
        match builder {
            Builder::Node { id, label } => {
                arrays.push(Arc::new(id.finish()));
                arrays.push(Arc::new(label.finish()));
            }
            Builder::TextArray(list) => arrays.push(Arc::new(list.finish())),
            Builder::Float(floats) => arrays.push(Arc::new(floats.finish())),
            Builder::Int(ints) => arrays.push(Arc::new(ints.finish())),
            Builder::Text(text) => arrays.push(Arc::new(text.finish())),
        }
    }
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    out.write(&batch).unwrap();
}
