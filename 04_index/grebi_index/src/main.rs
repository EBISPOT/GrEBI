
use grebi_shared::get_id;
use grebi_shared::json_lexer::JsonToken;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::io::BufRead;
use std::io::BufWriter;
use std::io::BufWrite;
use std::io::Write;
use std::io;
use std::iter::Map;
use grebi_shared::get_subjects;
use clap::Parser;

use grebi_shared::slice_merged_entity::SlicedEntity;
use grebi_shared::slice_merged_entity::SlicedReified;
use grebi_shared::slice_merged_entity::SlicedProperty;
use grebi_shared::json_lexer::{JsonTokenType};
use serde_json::json;


#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    out_metadata_json_path: String,

    #[arg(long)]
    out_subjects_txt_path: String
}

fn main() {

    let args = Args::parse();

    let start_time = std::time::Instant::now();

    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);

    let mut entity_props_to_count:HashMap<Vec<u8>,i64> = HashMap::new();
    let mut edge_props_to_count:HashMap<Vec<u8>,i64> = HashMap::new();

    let mut metadata_writer = BufWriter::new(File::create(&args.out_metadata_json_path).unwrap());
    let mut subjects_writer = BufWriter::new(File::create(&args.out_subjects_txt_path).unwrap());

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

        subjects_writer.write_all(&id).unwrap();
        subjects_writer.write_all(b"\n").unwrap();

        let sliced = SlicedEntity::from_json(&line);

        sliced.props.iter().for_each(|prop| {

            let prop_key = prop.key.to_vec();

            let count = entity_props_to_count.entry(prop_key).or_insert(0);
            *count += 1;

            if prop.kind == JsonTokenType::StartObject {

                let prop_val = prop.value.to_vec();

                let reified = SlicedReified::from_json(&prop_val);

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
    eprintln!("Extracting IDs took {} seconds", start_time.elapsed().as_secs());




    let start_time3 = std::time::Instant::now();

    metadata_writer.write_all(
    serde_json::to_string_pretty(&json!({
        "entity_props": entity_props_to_count.iter().map(|(k,v)| {
                return (String::from_utf8(k.to_vec()).unwrap(), json!({
                    "count": v
                }))
        }).collect::<HashMap<String,serde_json::Value>>(),
        "edge_props": edge_props_to_count.iter().map(|(k,v)| {
                return (String::from_utf8(k.to_vec()).unwrap(), json!({
                    "count": v
                }))
        }).collect::<HashMap<String,serde_json::Value>>()
    })).unwrap().as_bytes());
    
    eprintln!("Building metadata took {} seconds", start_time3.elapsed().as_secs());

}
