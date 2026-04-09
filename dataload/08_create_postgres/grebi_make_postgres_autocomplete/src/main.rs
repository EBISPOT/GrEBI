use std::io::{self, BufRead, BufWriter};

use grebi_shared::pgcopy::PgCopyWriter;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

/// Read one label per line from stdin, write PGCOPY binary with a single TEXT column.
fn main() {
    let stdin = io::stdin().lock();
    let reader = io::BufReader::new(stdin);
    let stdout = io::stdout().lock();
    let mut pgw = PgCopyWriter::new(BufWriter::new(stdout));

    for line in reader.lines() {
        let line = line.unwrap();
        if line.is_empty() {
            continue;
        }
        pgw.begin_row(1);
        pgw.write_text(&line);
    }

    let n = pgw.finish();
    eprintln!("grebi_make_postgres_autocomplete: wrote {} rows", n);
}
