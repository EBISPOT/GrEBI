
use std::io::{BufWriter, self, BufReader};
use clap::Parser;

mod write_associations;
mod write_studies;
mod check_headers;
mod remove_empty_fields;

use crate::write_associations::write_associations;
use crate::write_studies::write_studies;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    datasource_name: String,

    #[arg(long)]
    filename: String,
}

fn main() {

    let args = Args::parse();

    let stdin = io::stdin().lock();
    let reader = BufReader::new(stdin);

    let stdout = io::stdout().lock();
    let mut output_nodes = BufWriter::new(stdout);

    let mut csv_reader =
        csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .trim(csv::Trim::All)
        .from_reader(reader);

    if args.filename.contains("gwas-catalog-associations") {
        eprintln!("GWAS ingest: writing associations");
        write_associations(&mut csv_reader, &mut output_nodes, &args.datasource_name);
    } else if args.filename.contains("gwas-catalog-studies") {
        eprintln!("GWAS ingest: writing studies");
        write_studies(&mut csv_reader, &mut output_nodes, &args.datasource_name);
    } else {
        panic!("GWAS ingest: Unknown filename: {}", args.filename);
    }
}


