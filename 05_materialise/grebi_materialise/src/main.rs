
use std::ascii::escape_default;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::BufWriter;
use std::io::BufReader;
use std::io::Write;
use std::io;
use std::io::BufRead;
use std::io::StdoutLock;
use std::mem::transmute;
use sha1::{Sha1, Digest};

use clap::Parser;
use flate2::write::GzEncoder;
use flate2::Compression;
use grebi_shared::find_strings;
use grebi_shared::json_lexer::JsonTokenType;
use grebi_shared::json_parser;
use grebi_shared::load_metadata_mapping_table;
use grebi_shared::load_metadata_mapping_table::Metadata;
use grebi_shared::prefix_map::PrefixMap;

use grebi_shared::slice_merged_entity::SlicedPropertyValue;
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
    out_edges_jsonl: String,

    #[arg(long)]
    exclude: String
}

fn main() -> std::io::Result<()> {

    let args = Args::parse();

    let start_time = std::time::Instant::now();

    let node_metadata = load_metadata_mapping_table::load_metadata_mapping_table(&args.in_metadata_jsonl);

    let exclude:BTreeSet<Vec<u8>> = args.exclude.split(",").map(|s| s.to_string().as_bytes().to_vec()).collect();


    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);

    let edges_file = File::create(args.out_edges_jsonl).unwrap();
    let mut edges_writer = BufWriter::new(edges_file);

    let stdout = io::stdout().lock();
    let mut nodes_writer = BufWriter::new(stdout);

    let mut n_nodes:i64 = 0;

    loop {
        let mut line: Vec<u8> = Vec::new();
        reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            break;
        }
        if line[line.len() - 1] == b'\n' {
            line.pop();
        }

        let sliced = SlicedEntity::from_json(&line);

        n_nodes = n_nodes + 1;
        if n_nodes % 1000000 == 0 {
            eprintln!("... written {} nodes", n_nodes);
        }

        sliced.props.iter().for_each(|prop| {
            for val in &prop.values {
                maybe_write_edge(sliced.id, prop, &val, &mut edges_writer, &exclude, &node_metadata, &val.datasources);
            }
        });

        let _refs = {
            let mut res:Map<String,Value> = Map::new();
            for (start,end) in find_strings(&line) {
                let maybe_id = &line[start..end];
                let id_as_str = String::from_utf8_lossy(maybe_id).to_string();
                if !res.contains_key(&id_as_str) {
                    let metadata = node_metadata.get(maybe_id);
                    if metadata.is_some() {
                        res.insert(id_as_str, serde_json::from_slice(metadata.unwrap().json.as_slice()).unwrap());
                    }
                }
            }
            res
        };

        nodes_writer.write_all(&line[0..line.len()-1] /* skip closing bracket */).unwrap();
        nodes_writer.write_all(b",\"_refs\":").unwrap();
        nodes_writer.write_all(serde_json::to_string(&_refs).unwrap().as_bytes()).unwrap();
        nodes_writer.write_all(b"}\n").unwrap();
    }

    edges_writer.flush().unwrap();

    eprintln!("materialise took {} seconds", start_time.elapsed().as_secs());

    Ok(())
}

fn maybe_write_edge(from_id:&[u8], prop: &SlicedProperty, val:&SlicedPropertyValue,  edges_writer: &mut BufWriter<File>, exclude:&BTreeSet<Vec<u8>>, node_metadata:&BTreeMap<Vec<u8>, Metadata>, datasources:&Vec<&[u8]>) {

    if prop.key.eq(b"id") || prop.key.starts_with(b"grebi:") || exclude.contains(prop.key) {
        return;
    }

    if val.kind == JsonTokenType::StartObject {

        let reified = SlicedReified::from_json(&val.value);

        if reified.is_some() {
            let reified_u = reified.unwrap();
            if reified_u.value_kind == JsonTokenType::StartString {
                let buf = &reified_u.value.to_vec();
                let str = JsonParser::parse(&buf).string();
                let exists = node_metadata.contains_key(str);
                if exists {
                    write_edge(from_id, str, prop.key,  Some(&reified_u.props), edges_writer,  node_metadata, &datasources);
                }
            } else {
                // panic!("unexpected kind: {:?}", reified_u.value_kind);
            }
        } 
 
    } else if val.kind == JsonTokenType::StartString {

        let buf = &val.value.to_vec();
        let str = JsonParser::parse(&buf).string();
        let exists = node_metadata.contains_key(str);

        if exists {
            write_edge(from_id, str, prop.key, None, edges_writer, node_metadata, &datasources);
        }

    } else if val.kind == JsonTokenType::StartArray {

        // panic!("unexpected array, value: {:?}", String::from_utf8_lossy(prop.value));

    } else {

        // panic!("unexpected kind: {:?}", prop.kind);

    }

}

fn write_edge(from_id: &[u8], to_id: &[u8], edge:&[u8], edge_props:Option<&Vec<SlicedProperty>>, edges_writer: &mut BufWriter<File>, node_metadata:&BTreeMap<Vec<u8>,Metadata>, datasources:&Vec<&[u8]>) {

    let mut buf = Vec::new();

    buf.extend(b"\"grebi:type\":\"");
    buf.extend(edge);
    buf.extend(b"\",\"grebi:from\":\"");
    buf.extend(from_id);
    buf.extend(b"\",\"grebi:to\":\"");
    buf.extend(to_id);
    buf.extend(b"\",\"grebi:datasources\":[");

    let mut is_first_ds = true;
    datasources.iter().for_each(|ds| {
        if is_first_ds {
            is_first_ds = false;
        } else {
            buf.extend(b",");
        }
        buf.extend(b"\"");
        buf.extend(ds.iter());
        buf.extend(b"\"");
    });
    buf.extend(b"]");

    if edge_props.is_some() {
        for prop in edge_props.unwrap() {
            buf.extend(b",");
            buf.extend(b"\"");
            buf.extend(prop.key);
            buf.extend(b"\":");
            buf.extend(prop.values_slice);
        }
    }

    let _refs = {
        let mut res:Map<String,Value> = Map::new();
        for (start,end) in find_strings(&buf) {
            let maybe_id = &buf[start..end];
            let id_as_str = String::from_utf8_lossy(maybe_id).to_string();
            if !res.contains_key(&id_as_str) {
                let metadata = node_metadata.get(maybe_id);
                if metadata.is_some() {
                    res.insert(id_as_str, serde_json::from_slice(metadata.unwrap().json.as_slice()).unwrap());
                }
            }
        }
        res
    };

    // sha1 not for security, just as a simple way to assign a unique
    // id to the edge that will be reproducible between dataloads
    //
    let mut hasher = Sha1::new();
    hasher.update(&buf);
    let hash = hasher.finalize();

    edges_writer.write_all(b"{\"grebi:edgeId\":\"").unwrap();
    edges_writer.write_all(hex::encode(hash).as_bytes()).unwrap();
    edges_writer.write_all(b"\",").unwrap();
    edges_writer.write_all(&buf).unwrap();
    edges_writer.write_all(b",\"_refs\":").unwrap();
    edges_writer.write_all(serde_json::to_string(&_refs).unwrap().as_bytes()).unwrap();
    edges_writer.write_all(b"}\n").unwrap();
}


