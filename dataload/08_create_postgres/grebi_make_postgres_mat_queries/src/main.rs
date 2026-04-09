use std::io::{self, BufRead, BufWriter};

use clap::Parser;
use grebi_shared::pgcopy::PgCopyWriter;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    query_id: String,
}

/// Read JSONL from stdin, write PGCOPY binary with columns: query_id TEXT, row_number INT, data JSONB.
fn main() {
    let args = Args::parse();
    let stdin = io::stdin().lock();
    let reader = io::BufReader::new(stdin);
    let stdout = io::stdout().lock();
    let mut pgw = PgCopyWriter::new(BufWriter::new(stdout));

    let mut row_number: i32 = 0;

    for line in reader.lines() {
        let line = line.unwrap();
        if line.is_empty() {
            continue;
        }
        row_number += 1;
        pgw.begin_row(3);
        pgw.write_text(&args.query_id);
        pgw.write_int32(row_number);
        pgw.write_jsonb(&line);
    }

    let n = pgw.finish();
    eprintln!("grebi_make_postgres_mat_queries ({}): wrote {} rows", args.query_id, n);
}
