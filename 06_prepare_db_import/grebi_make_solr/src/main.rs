

use std::ascii::escape_default;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::BufWriter;
use std::io::BufReader;
use std::io::Write;
use std::io;
use std::io::BufRead;
use std::mem::transmute;

use clap::Parser;
use grebi_shared::find_strings;
use grebi_shared::json_lexer::JsonTokenType;
use grebi_shared::json_parser;
use grebi_shared::load_metadata_mapping_table;
use grebi_shared::load_metadata_mapping_table::Metadata;
use grebi_shared::prefix_map::PrefixMap;

use serde_json::Map;
use serde_json::Value;
use grebi_shared::slice_merged_entity::SlicedEntity;
use grebi_shared::slice_merged_entity::SlicedProperty;
use grebi_shared::slice_merged_entity::SlicedReified;

use grebi_shared::json_lexer::{lex, JsonToken };
use grebi_shared::json_parser::JsonParser;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    in_metadata_jsonl: String,

    #[arg(long)]
    in_summary_json: String,

    #[arg(long)]
    in_nodes_jsonl: String,

    #[arg(long)]
    in_edges_jsonl: String,

    #[arg(long)]
    out_nodes_jsonl_path: String,

    #[arg(long)]
    out_edges_jsonl_path: String,
}

fn main() -> std::io::Result<()> {

    let args = Args::parse();

    let start_time = std::time::Instant::now();

    let node_metadata = load_metadata_mapping_table::load_metadata_mapping_table(&args.in_metadata_jsonl);
    let summary:Value = serde_json::from_reader(File::open(args.in_summary_json).unwrap()).unwrap();

    let all_entity_props: Vec<String> = summary["entity_props"].as_object().unwrap().keys().cloned().collect();
    let all_edge_props: Vec<String> = summary["edge_props"].as_object().unwrap().keys().cloned().collect();

    let mut nodes_reader = BufReader::new(File::open(args.in_nodes_jsonl).unwrap());
    let mut edges_reader = BufReader::new(File::open(args.in_edges_jsonl).unwrap());

    let mut nodes_file = File::create(args.out_nodes_jsonl_path).unwrap();
    let mut nodes_writer =
        BufWriter::with_capacity(1024*1024*32,
            &nodes_file
        );

    let mut edges_file = File::create(args.out_edges_jsonl_path).unwrap();
    let mut edges_writer =
        BufWriter::with_capacity(1024*1024*32,
            &edges_file
        );

    let mut n_nodes:i64 = 0;

    loop {
        let mut line: Vec<u8> = Vec::new();
        nodes_reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            break;
        }
        if line[line.len() - 1] == b'\n' {
            line.pop();
        }

        write_solr_object(&line, &mut nodes_writer, &node_metadata);
    }

    loop {
        let mut line: Vec<u8> = Vec::new();
        edges_reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            break;
        }
        if line[line.len() - 1] == b'\n' {
            line.pop();
        }

        write_solr_object(&line, &mut edges_writer, &node_metadata);
    }

    nodes_writer.flush().unwrap();
    nodes_file.sync_all().unwrap();

    nodes_file.sync_all().unwrap();
    edges_file.sync_all().unwrap();

    eprintln!("prepare_db_import for solr took {} seconds", start_time.elapsed().as_secs());

    Ok(())
}

fn write_solr_object(line:&Vec<u8>, nodes_writer:&mut BufWriter<&File>, node_metadata:&BTreeMap<Vec<u8>,Metadata>) {

    let _refs = {
        let mut res:Map<String,Value> = Map::new();
        for (start,end) in find_strings(&line) {
            let maybe_id = &line[start..end];
            let metadata = node_metadata.get(maybe_id);
            if metadata.is_some() {
                res.insert(String::from_utf8_lossy(maybe_id).to_string(), serde_json::from_slice(metadata.unwrap().json.as_slice()).unwrap());
            }
        }
        res
    };

    let mut json:serde_json::Map<String,Value> = serde_json::from_slice(&line).unwrap();
    json.insert("_refs".to_string(), Value::Object(_refs));

    let mut out_json = serde_json::Map::new();
    out_json.insert("_json".to_string(), Value::String(serde_json::to_string(&json).unwrap()));

    for (k,v) in json.iter() {

        if k.eq("_refs") {
            continue; // we just added this for the _json field, don't want it indexed
        }

        // for internal fields just copy the value
        if k.starts_with("grebi:") {
            out_json.insert(k.to_string(), v.clone());
            continue;
        }

        let arr = v.as_array().unwrap();
        let mut new_arr:Vec<Value> = Vec::new();

        for i in 0..arr.len() {
            let el = &arr[i];
            new_arr.extend(value_to_solr(el, &node_metadata));
        }

        if new_arr.len() > 0 {
            out_json.insert(k.clone(), Value::Array(new_arr));
        }
    }

    nodes_writer.write_all(serde_json::to_string(&out_json).unwrap().as_bytes()).unwrap();
    nodes_writer.write_all(b"\n").unwrap();
}

fn value_to_solr(v:&Value, node_metadata:&BTreeMap<Vec<u8>,Metadata>) -> Vec<Value> {

    if v.is_array() {
        return v.as_array().map(|arr| {
            arr.iter().flat_map(|el| value_to_solr(el, node_metadata)).collect()
        }).unwrap();
    }

    if v.is_object() {
        let vobj = v.as_object().unwrap();
        if vobj.contains_key("grebi:value") {
            return value_to_solr(vobj.get("grebi:value").unwrap(), node_metadata);
        } else {
            return vec!();
        }
    }

    if v.is_string() {
        let metadata = node_metadata.get(v.as_str().unwrap().as_bytes());
        if metadata.is_some() {
            let metadata_u = metadata.unwrap();
            if metadata_u.name.is_some() {
                // add both the ID and its label
                return vec!(v.clone(), Value::String(String::from_utf8_lossy(&metadata_u.name.as_ref().unwrap()).to_string()));
            }
        } else {
            return vec!(v.clone());
        }
    }

    return vec!(v.clone());

}


