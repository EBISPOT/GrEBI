//! A two-column mapping table (id, comma-separated equivalent ids) as GrEBI
//! JSONL: each row is a node whose grebi:equivalentTo lists the equivalents.

use clap::Parser;
use serde_json::{json, Value};
use std::io::{self, BufRead, BufWriter, Write};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    datasource_name: String,
    /// Unused; the pipeline passes it to every ingest.
    #[arg(long)]
    filename: Option<String>,
}

fn main() {
    let args = Args::parse();
    let mut out = BufWriter::new(io::stdout().lock());
    for (n, line) in io::stdin().lock().lines().enumerate() {
        let line = line.unwrap();
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut columns = line.split('\t');
        let id = columns.next().unwrap();
        let equivalents: Vec<Value> = match columns.next() {
            Some(list) => list.split(',').map(|s| Value::String(s.to_string())).collect(),
            None => {
                eprintln!("line {}: expected an id and its comma-separated equivalents separated by a tab", n + 1);
                std::process::exit(1);
            }
        };
        let node = json!({"id": id, "grebi:datasource": args.datasource_name, "grebi:equivalentTo": equivalents});
        serde_json::to_writer(&mut out, &node).unwrap();
        out.write_all(b"\n").unwrap();
    }
    out.flush().unwrap();
}
