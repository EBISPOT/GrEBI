//! Query result rows, one JSON object per line on stdin, as CSV on stdout.
//!
//! The header is the union of the rows' keys in the order they are first seen,
//! so the input is read twice: once to learn the columns while the rows are
//! spilled to a temporary file, once to write them out. Memory stays flat
//! however many rows there are (the Python exporter this replaces held the
//! whole result set in a data frame).
//!
//! Cells: strings as they are, numbers as written in the JSON, booleans as
//! true/false, null and a missing key as an empty cell, arrays as their
//! elements joined with ";", objects as compact JSON.

use serde_json::{Map, Value};
use std::collections::HashSet;
use std::io::{self, BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};

fn cell(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::Array(items)) => items.iter().map(element).collect::<Vec<_>>().join(";"),
        Some(v) => element(v),
    }
}

fn element(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

fn parse(line: &str, line_number: usize) -> Map<String, Value> {
    match serde_json::from_str::<Value>(line) {
        Ok(Value::Object(row)) => row,
        Ok(_) => {
            eprintln!("line {} is not a JSON object", line_number);
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("line {} is not valid JSON: {}", line_number, e);
            std::process::exit(1);
        }
    }
}

fn main() -> io::Result<()> {
    let stdin = io::stdin();
    let mut spill = tempfile::tempfile()?;
    let mut columns: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut line_number = 0;
    {
        let mut spill_writer = BufWriter::new(&mut spill);
        for line in stdin.lock().lines() {
            let line = line?;
            line_number += 1;
            if line.trim().is_empty() {
                continue;
            }
            for key in parse(&line, line_number).keys() {
                if seen.insert(key.clone()) {
                    columns.push(key.clone());
                }
            }
            spill_writer.write_all(line.as_bytes())?;
            spill_writer.write_all(b"\n")?;
        }
        spill_writer.flush()?;
    }

    if columns.is_empty() {
        // no rows: no header either
        return Ok(());
    }
    spill.seek(SeekFrom::Start(0))?;
    let mut out = csv::Writer::from_writer(BufWriter::new(io::stdout().lock()));
    out.write_record(&columns)?;
    let mut line_number = 0;
    for line in BufReader::new(spill).lines() {
        let line = line?;
        line_number += 1;
        let row = parse(&line, line_number);
        out.write_record(columns.iter().map(|c| cell(row.get(c))))?;
    }
    out.flush()?;
    Ok(())
}
