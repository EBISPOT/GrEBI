
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, self, BufReader, StdinLock, StdoutLock, Write, BufRead, Read};
use std::ptr::eq;
use clap::Parser;
use grebi_shared::prefix_map::PrefixMap;
use grebi_shared::prefix_map::PrefixMapBuilder;
use grebi_shared::serialize_equivalence;
use serde_json::json;
use serde_yaml;

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
    let mut reader = BufReader::new(stdin);

    let datasource_name = args.datasource_name.as_str();

    let mut output_nodes = BufWriter::new(
        File::create(args.output_nodes.as_str()).unwrap());

    let mut output_equivalences = BufWriter::new(
         File::create(args.output_equivalences.as_str()).unwrap());

    let normalise:PrefixMap = {
        let rdr = BufReader::new( std::fs::File::open("/home/james/grebi2/prefix_map_normalise.json").unwrap() );
        let mut builder = PrefixMapBuilder::new();
        serde_json::from_reader::<_, HashMap<String, String>>(rdr).unwrap().into_iter().for_each(|(k, v)| {
            builder.add_mapping(k, v);
        });
        builder.build()
    };



}



