
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, self, BufReader, StdinLock, StdoutLock, Write};
use std::ptr::eq;
use clap::Parser;
use grebi_shared::prefix_map::PrefixMap;
use grebi_shared::prefix_map::PrefixMapBuilder;
use grebi_shared::serialize_equivalence;
use serde_json::json;

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

    #[arg(long)]
    output_nodes:String,

    #[arg(long)]
    output_equivalences:String
}

fn main() {

    let args = Args::parse();

    let stdin = io::stdin().lock();
    let reader = BufReader::new(stdin);


    let mut output_nodes = BufWriter::new(
        File::create(args.output_nodes.as_str()).unwrap());

    let mut output_equivalences = BufWriter::new(
         File::create(args.output_equivalences.as_str()).unwrap());
    // output_equivalences.write_all(b"subject_id\tobject_id\n").unwrap();



    let normalise = {
        let rdr = BufReader::new( std::fs::File::open("prefix_map_normalise.json").unwrap() );
        let mut builder = PrefixMapBuilder::new();
        serde_json::from_reader::<_, HashMap<String, String>>(rdr).unwrap().into_iter().for_each(|(k, v)| {
            builder.add_mapping(k, v);
        });
        builder.build()
    };


    let mut csv_reader =
        csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .trim(csv::Trim::All)
        .from_reader(reader);

    if args.filename.starts_with("gwas-catalog-associations") {
        eprintln!("GWAS ingest: writing associations");
        write_associations(&mut csv_reader, &mut output_nodes, &mut output_equivalences, &args.datasource_name, &normalise);
    } else if args.filename.starts_with("gwas-catalog-studies") {
        eprintln!("GWAS ingest: writing studies");
        write_studies(&mut csv_reader, &mut output_nodes, &mut output_equivalences, &args.datasource_name, &normalise);
    } else {
        panic!("GWAS ingest: Unknown filename: {}", args.filename);
    }
}


