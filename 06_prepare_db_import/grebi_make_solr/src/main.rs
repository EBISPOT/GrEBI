

use std::ascii::escape_default;
use std::borrow::Borrow;
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

        write_solr_object(&line, &mut nodes_writer);
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

        write_solr_object(&line, &mut edges_writer);
    }

    nodes_writer.flush().unwrap();
    nodes_file.sync_all().unwrap();

    nodes_file.sync_all().unwrap();
    edges_file.sync_all().unwrap();

    eprintln!("prepare_db_import for solr took {} seconds", start_time.elapsed().as_secs());

    Ok(())
}

fn write_solr_object(line:&Vec<u8>, nodes_writer:&mut BufWriter<&File>) {

    let json:serde_json::Map<String,Value> = serde_json::from_slice(&line).unwrap();
    let mut out_json = serde_json::Map::new();

    let refs = json.get("_refs").unwrap().as_object().unwrap();

    for (k,v) in json.iter() {

        if k.eq("_refs") {
            continue;
        }

        // some special properties aren't structured like normal properties, so
        // just copy the value
        //
        if k.eq("grebi:nodeId") ||
            k.eq("grebi:edgeId") ||
            k.eq("grebi:datasources") ||
            k.eq("grebi:fromNodeId") ||
            k.eq("grebi:toNodeId") ||
            k.eq("grebi:fromSourceId") ||
            k.eq("grebi:toSourceId") ||
            k.eq("grebi:subgraph") ||
            k.eq("grebi:sourceIds") ||
            k.eq("grebi:displayType") ||
            ( k.eq("grebi:type") && !v.is_array() /* edge types are singular */ )
            {
            out_json.insert(escape_key(k), v.clone());
            continue;
        }

        if !v.is_array() {
            panic!("expected array for property value: {} in {}", k, String::from_utf8_lossy(line));
            continue;
        }

        let arr = v.as_array().unwrap();
        let mut new_arr:Vec<Value> = Vec::new();

        for i in 0..arr.len() {
            let el = &arr[i];
            new_arr.extend(value_to_solr(el, &refs));
        }

        if new_arr.len() > 0 {
            out_json.insert(escape_key(k), Value::Array(new_arr));
        }
    }

    nodes_writer.write_all(serde_json::to_string(&out_json).unwrap().as_bytes()).unwrap();
    nodes_writer.write_all(b"\n").unwrap();
}

fn value_to_solr(v:&Value, refs:&Map<String,Value>) -> Vec<Value> {

    if v.is_array() {
        return v.as_array().map(|arr| {
            arr.iter().flat_map(|el| value_to_solr(el, &refs)).collect()
        }).unwrap();
    }

    if v.is_object() {
        let vobj = v.as_object().unwrap();
        if vobj.contains_key("grebi:value") {
            return value_to_solr(vobj.get("grebi:value").unwrap(), &refs);
        } else {
            return vec!();
        }
    }

    if v.is_string() {
        let metadata = refs.get(&v.as_str().unwrap().to_owned());
        if metadata.is_some() {
            let metadata_u = metadata.unwrap();
            let names = metadata_u.get("grebi:name");
            if names.is_some() {
                // add both the ID and its labels
                return {
                    let mut res = vec!(v.clone());
                    for label in names.unwrap().as_array().unwrap() {
                        res.push(label.clone());
                    }
                    res
                }
            }
        } else {
            return vec!(v.clone());
        }
    }

    return vec!(v.clone());

}

fn escape_key(k:&str) -> String {
    let mut res = String::new();

    for c in k.chars() {
        if c == ':' {
            res.push_str("__");
        } else {
            res.push(c);
        }
    }
    return res;
}

