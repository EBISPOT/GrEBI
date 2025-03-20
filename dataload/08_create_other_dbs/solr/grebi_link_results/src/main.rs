



use std::collections::{HashMap, HashSet, BTreeSet};
use std::fs::File;
use std::{env, io};
use std::io::{BufRead, BufReader };
use std::io::{Write, BufWriter};
use grebi_shared::json_lexer::{lex, JsonTokenType};
use grebi_shared::json_parser::JsonParser;
use clap::Parser;
use sha1::{Sha1, Digest};

use serde_json::Value;

use grebi_shared::load_metadata_mapping_table;
use grebi_shared::load_groups_txt::load_id_to_group_mapping;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    in_metadata_jsonl: String,

    #[arg(long)]
    groups_txt: String,
    

}

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() {

    let args = Args::parse();

    let id_to_group = load_id_to_group_mapping(&args.groups_txt);

    let node_metadata = load_metadata_mapping_table::load_metadata_mapping_table(&args.in_metadata_jsonl);

    let start_time = std::time::Instant::now();

    let stdin = io::stdin();
    let handle = stdin.lock();
    let mut reader = BufReader::new(handle);

    let stdout = io::stdout().lock();
    let mut writer = BufWriter::new(stdout);

    loop {
        let mut line: Vec<u8> = Vec::new();
        reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            break;
        }

        let mut json:serde_json::Map<String,Value> = serde_json::from_slice(&line).unwrap();
        let mut refs = serde_json::Map::new();
        let mut nodeids:HashSet<String> = HashSet::new();

        for (k,v) in json.iter() {

            if !v.is_string() {
                continue;
            }

            let k_group = id_to_group.get(k.as_str().as_bytes());

            if k_group.is_some() {
                let metadata = node_metadata.get(k_group.unwrap());
                if metadata.is_some() {
                    refs.insert(k.as_str().to_string(), serde_json::from_slice( metadata.unwrap().json.as_slice() ).unwrap() );
                    nodeids.insert(String::from_utf8(k_group.unwrap().to_vec()).unwrap().to_string()); 
                }
            }

            let v_group = id_to_group.get(v.as_str().unwrap().as_bytes());

            if v_group.is_some() {
                let metadata = node_metadata.get(v_group.unwrap());
                if metadata.is_some() {
                    refs.insert(v.as_str().unwrap().to_string(), serde_json::from_slice( metadata.unwrap().json.as_slice() ).unwrap() );
                    nodeids.insert(String::from_utf8(v_group.unwrap().to_vec()).unwrap().to_string()); 
                }
            }
        }

        json.insert("_refs".to_string(), Value::String(Value::Object(refs).to_string())    );
	json.insert("_node_ids".to_string(), Value::Array( nodeids.iter().map(|id| Value::String(id.clone())).collect()));

    // sha1 not for security, just as a simple way to assign a unique
    // id to the result to use to reference it in the api
    //
    let mut hasher = Sha1::new();
    hasher.update(&line);
    let hash = hasher.finalize();
	json.insert("id".to_string(), Value::String(hex::encode(hash)));

        writer.write_all(Value::Object(json).to_string().as_bytes()).unwrap();
        writer.write_all("\n".as_bytes()).unwrap();
        
    }

    eprintln!("completed id to group mapping in {}", start_time.elapsed().as_secs());

}


