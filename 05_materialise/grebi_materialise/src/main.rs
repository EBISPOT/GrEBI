
use std::ascii::escape_default;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use std::io::BufWriter;
use std::io::BufReader;
use std::io::Write;
use std::io;
use std::io::BufRead;
use std::io::StdoutLock;
use std::mem::transmute;
use grebi_shared::load_groups_txt::load_id_to_group_mapping;
use sha1::{Sha1, Digest};
use serde_json::json;

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
    out_summary_json: String,

    #[arg(long)]
    groups_txt: String,

    #[arg(long)]
    exclude: String,

    #[arg(long)]
    exclude_self_referential: String
}


type EdgeSummaryTable = HashMap<
    String, /* src node type signature */
    HashMap<
        String /* edge type */,
        HashMap<
            String, /* dest node type signature */
            HashMap<
                String, /* set of datasources */
                u64 /* count */
            >
        >
    >
>;

fn main() -> std::io::Result<()> {

    let args = Args::parse();

    let start_time = std::time::Instant::now();

    // we need to map the ids given to --exclude to their identifier groups
    // as the caller of this program won't know for sure which id the property actually got
    //
    let mut id_to_group:HashMap<Vec<u8>, Vec<u8>> = load_id_to_group_mapping(&args.groups_txt);

    let exclude:BTreeSet<Vec<u8>> = args.exclude.split(",").map(|s| {
        let group = id_to_group.get(s.as_bytes());
        if group.is_some() {
            group.unwrap().clone()
        } else {
            s.as_bytes().to_vec()
        }
    }).collect();

    let exclude_self_ref:BTreeSet<Vec<u8>> = args.exclude_self_referential.split(",").map(|s| {
        let group = id_to_group.get(s.as_bytes());
        if group.is_some() {
            group.unwrap().clone()
        } else {
            s.as_bytes().to_vec()
        }
    }).collect();

    id_to_group.clear();
    id_to_group.shrink_to(0);

    let node_metadata = load_metadata_mapping_table::load_metadata_mapping_table(&args.in_metadata_jsonl);

    let mut types_to_count:HashMap<Vec<u8>,i64> = HashMap::new();
    {
        let summary_json:Map<String, Value> = serde_json::from_reader(File::open(&args.in_summary_json).unwrap()).unwrap();
        for (k, v) in summary_json["types"].as_object().unwrap() {
            types_to_count.insert(k.as_bytes().to_vec(), v.as_object().unwrap()["count"].as_i64().unwrap());
        }
    }

    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);

    let edges_file = File::create(args.out_edges_jsonl).unwrap();
    let mut edges_writer = BufWriter::new(edges_file);

    let stdout = io::stdout().lock();
    let mut nodes_writer = BufWriter::new(stdout);

    let summary_file = File::create(args.out_summary_json).unwrap();
    let mut summary_writer = BufWriter::new(summary_file);

    let mut edge_summary:EdgeSummaryTable = HashMap::new();

    let mut all_entity_props:BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut all_edge_props:BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut all_types:BTreeSet<Vec<u8>> = BTreeSet::new();

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

        let mut rarest_type:Option<Vec<u8>> = None;
        let mut rarest_type_count:i64 = std::i64::MAX;

        sliced.props.iter().for_each(|prop| {

            let prop_key = prop.key;

            if prop_key.eq(b"grebi:type") {
                for val in &prop.values {
                    if val.kind == JsonTokenType::StartString {
                        let buf = &val.value.to_vec();
                        let str = JsonParser::parse(&buf).string();
                        all_types.insert(str.to_vec());

                        let count = types_to_count.get(str);
                        if count.is_some() {
                            if *count.unwrap() < rarest_type_count {
                                rarest_type = Some(str.to_vec());
                                rarest_type_count = *count.unwrap();
                            }
                        }
                    }
                }

            }

            all_entity_props.insert(prop_key.to_vec());

            for val in &prop.values {
                maybe_write_edge(sliced.id, prop, &val, &mut edges_writer, &exclude, &exclude_self_ref, &node_metadata, &val.datasources, sliced.subgraph, &mut edge_summary, &mut all_edge_props);
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
        if rarest_type.is_some() {
            nodes_writer.write_all(b",\"grebi:displayType\":\"").unwrap();
            nodes_writer.write_all(&rarest_type.unwrap()).unwrap();
            nodes_writer.write_all(b"\"").unwrap();
        }
        nodes_writer.write_all(b",\"_refs\":").unwrap();
        nodes_writer.write_all(serde_json::to_string(&_refs).unwrap().as_bytes()).unwrap();
        nodes_writer.write_all(b"}\n").unwrap();
    }

    edges_writer.flush().unwrap();

    eprintln!("materialise took {} seconds", start_time.elapsed().as_secs());

    let mut entity_prop_defs:Map<String,Value> = Map::new();

    for prop in all_entity_props {
        let def = node_metadata.get(&prop);
        if def.is_some() {
            entity_prop_defs.insert(String::from_utf8_lossy(&prop).to_string(), serde_json::from_slice::<Value>(def.unwrap().json.as_slice()).unwrap());
        }
    }

   let mut edge_prop_defs:Map<String,Value> = Map::new();

    for prop in all_edge_props {
        let def = node_metadata.get(&prop);
        if def.is_some() {
            edge_prop_defs.insert(String::from_utf8_lossy(&prop).to_string(), serde_json::from_slice::<Value>(def.unwrap().json.as_slice()).unwrap());
        }
    }
   let mut type_defs:Map<String,Value> = Map::new();

    for t in all_types {
        let def = node_metadata.get(&t);
        if def.is_some() {
            type_defs.insert(String::from_utf8_lossy(&t).to_string(), serde_json::from_slice::<Value>(def.unwrap().json.as_slice()).unwrap());
        }
    }

    summary_writer.write_all(serde_json::to_string_pretty(&json!({
        "entity_prop_defs": entity_prop_defs,
        "edge_prop_defs": edge_prop_defs,
        "types": type_defs,
        "edges": edge_summary
    })).unwrap().as_bytes()).unwrap();

    summary_writer.flush().unwrap();

    Ok(())
}

fn maybe_write_edge(from_id:&[u8], prop: &SlicedProperty, val:&SlicedPropertyValue,  edges_writer: &mut BufWriter<File>, exclude:&BTreeSet<Vec<u8>>, exclude_self_ref:&BTreeSet<Vec<u8>>, node_metadata:&BTreeMap<Vec<u8>, Metadata>, datasources:&Vec<&[u8]>, subgraph:&[u8], edge_summary: &mut EdgeSummaryTable, all_edge_props: &mut BTreeSet<Vec<u8>>) {

    if prop.key.eq(b"id") || prop.key.starts_with(b"grebi:") || exclude.contains(prop.key) {
        return;
    }

    if val.kind == JsonTokenType::StartObject {

        let reified = SlicedReified::from_json(&val.value);

        if reified.is_some() {
            let reified_u = reified.unwrap();
		reified_u.props.iter().for_each(|prop| {
		    let prop_key = prop.key.to_vec();
            all_edge_props.insert(prop_key);
		});
            if reified_u.value_kind == JsonTokenType::StartString {
                let buf = &reified_u.value.to_vec();
                let str = JsonParser::parse(&buf).string();
                let exists = node_metadata.contains_key(str);
                if exists {
                    if from_id.eq(str) && exclude_self_ref.contains(prop.key) {
                        return;
                    }
                    write_edge(from_id, str, prop.key,  Some(&reified_u.props), edges_writer,  node_metadata, &datasources, &subgraph, edge_summary);
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
            if from_id.eq(str) &&  exclude_self_ref.contains(prop.key) {
                return;
            }
            write_edge(from_id, str, prop.key, None, edges_writer, node_metadata, &datasources, &subgraph, edge_summary);
        }

    } else if val.kind == JsonTokenType::StartArray {

        // panic!("unexpected array, value: {:?}", String::from_utf8_lossy(prop.value));

    } else {

        // panic!("unexpected kind: {:?}", prop.kind);

    }

}

fn write_edge(from_id: &[u8], to_id: &[u8], edge:&[u8], edge_props:Option<&Vec<SlicedProperty>>, edges_writer: &mut BufWriter<File>, node_metadata:&BTreeMap<Vec<u8>,Metadata>, datasources:&Vec<&[u8]>, subgraph:&[u8], edge_summary:&mut EdgeSummaryTable) {
 
    let mut buf = Vec::new();

    buf.extend(b"\"grebi:type\":\"");
    buf.extend(edge);
    buf.extend(b"\",\"grebi:subgraph\":\"");
    buf.extend(subgraph);
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

    let _refs:Map<String,Value> = {
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

    let from_type_signature:String = get_type_signature_from_metadata_json(_refs.get(&String::from_utf8_lossy(from_id).to_string()).unwrap());
    let to_type_signature:String = get_type_signature_from_metadata_json(_refs.get(&String::from_utf8_lossy(to_id).to_string()).unwrap());
    let datasources_signature:String =  datasources.iter().map(|ds| String::from_utf8_lossy(ds).to_string()).collect::<Vec<String>>().join(",");

    let edge_summary_edges = edge_summary.entry(from_type_signature).or_insert(HashMap::new());
    let count:&mut u64 = edge_summary_edges
        .entry(String::from_utf8_lossy(edge).to_string())
        .or_insert(HashMap::new())
            .entry(to_type_signature)
            .or_insert(HashMap::new())
                .entry(datasources_signature)
                .or_insert(0);

    *count = *count + 1;

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




fn get_type_signature_from_metadata_json(json:&Value) -> String {
    let mut t:Vec<&str> = json.as_object().unwrap()
        .get("grebi:type").unwrap()
        .as_array().unwrap()
        .iter()
        .map(|val| val.as_str().unwrap())
        .collect();
    t.sort();
    return t.join(",").to_string();
}
