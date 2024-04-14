
use grebi_shared::get_id;
use grebi_shared::json_lexer::JsonToken;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::io::BufRead;
use std::io::Write;
use std::io;
use std::iter::Map;
use grebi_shared::get_subjects;
use clap::Parser;
use rocksdb::DB;
use rocksdb::Options;

mod slice_entity;

use slice_entity::SlicedEntity;
use slice_entity::SlicedReified;
use slice_entity::SlicedProperty;
use grebi_shared::json_lexer::{JsonTokenType};
use serde_json::json;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    rocksdb_path: String
}

fn main() {

    let args = Args::parse();

    let start_time = std::time::Instant::now();

    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);

    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options.prepare_for_bulk_load();
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);
    options.set_max_open_files(900); // codon limit is 1024 per process

     let db = DB::open(&options, args.rocksdb_path).unwrap();

     let mut entity_props_to_count:HashMap<Vec<u8>,i64> = HashMap::new();
     let mut edge_props_to_count:HashMap<Vec<u8>,i64> = HashMap::new();

    let mut line:Vec<u8> = Vec::new();
    let mut n:i64 = 0;

    loop {

        line.clear();
        reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            eprintln!("saw {} subjects", n);
            break;
        }

        let id = get_id(&line);
        db.put(&id, &line).unwrap();



        let sliced = slice_entity::SlicedEntity::from_json(&line);

        sliced.props.iter().for_each(|prop| {

            let prop_key = prop.key.to_vec();

            let count = entity_props_to_count.entry(prop_key).or_insert(0);
            *count += 1;

            if prop.kind == JsonTokenType::StartObject {

                let prop_val = prop.value.to_vec();

                let reified = slice_entity::SlicedReified::from_json(&prop_val);

                if reified.is_some() {
                    reified.unwrap().props.iter().for_each(|prop| {
                        let prop_key = prop.key.to_vec();

                        let count2 = edge_props_to_count.entry(prop_key).or_insert(0);
                        *count2 += 1;
                    });
                }
            }
        });



        n = n + 1;

        if n % 1000000 == 0 {
            eprintln!("{}", n);
        }
    }

    eprintln!("Building took {} seconds", start_time.elapsed().as_secs());

    let start_time2 = std::time::Instant::now();

    db.compact_range(None::<&[u8]>, None::<&[u8]>);

    eprintln!("Compacting took {} seconds", start_time2.elapsed().as_secs());

    println!("{}", serde_json::to_string_pretty(&json!({
        "entity_props": entity_props_to_count.iter().map(|(k,v)| (String::from_utf8(k.to_vec()).unwrap(),v)).collect::<HashMap<String,&i64>>(),
        "edge_props": edge_props_to_count.iter().map(|(k,v)| (String::from_utf8(k.to_vec()).unwrap(),v)).collect::<HashMap<String,&i64>>()
    })).unwrap());

}
