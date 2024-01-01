
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, self, BufReader, StdinLock, StdoutLock, Write};
use std::ptr::eq;
use clap::Parser;
use grebi_shared::prefix_map::PrefixMap;
use grebi_shared::prefix_map::PrefixMapBuilder;
use grebi_shared::serialize_equivalence;
use serde_json::json;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(short, long)]
    datasource_name: String,

    #[arg(short, long)]
    filename: String,

    #[arg(short, long)]
    output_nodes:String,

    #[arg(short, long)]
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



    let rdr = BufReader::new( std::fs::File::open("prefix_map.json").unwrap() );
    let mut builder = PrefixMapBuilder::new();
    serde_json::from_reader::<_, HashMap<String, String>>(rdr).unwrap().into_iter().for_each(|(k, v)| {
        builder.add_mapping(k, v);
    });
    let prefix_map = builder.build();



    // let mut csv_reader =
    //     csv::ReaderBuilder::new()
    //     .delimiter(b'\t')
    //     .has_headers(true)

    Ok(())
}
