
use std::collections::{HashMap, BTreeMap};
use std::fs::File;
use std::io::{BufWriter, self, BufReader, StdinLock, StdoutLock, Write};
use std::ptr::eq;
use clap::Parser;
use grebi_shared::prefix_map::PrefixMap;
use grebi_shared::prefix_map::PrefixMapBuilder;
use grebi_shared::serialize_equivalence;
use struson::reader::{JsonStreamReader, JsonReader};
use serde_json::Value;
use serde_json::Map;

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

    eprintln!("args: {:?}", args);

    let stdin = io::stdin().lock();
    let reader = BufReader::new(stdin);

    let datasource_name = args.datasource_name.as_str();

    let normalise = {
        let rdr = BufReader::new( std::fs::File::open("prefix_map_normalise.json").unwrap() );
        let mut builder = PrefixMapBuilder::new();
        serde_json::from_reader::<_, HashMap<String, String>>(rdr).unwrap().into_iter().for_each(|(k, v)| {
            builder.add_mapping(k, v);
        });
        builder.build()
    };


}
