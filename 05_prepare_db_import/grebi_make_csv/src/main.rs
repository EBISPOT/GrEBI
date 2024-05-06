

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

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    in_metadata_jsonl: String,

    #[arg(long)]
    in_summary_json: String,

    #[arg(long)]
    out_nodes_csv_path: String,

    #[arg(long)]
    out_edges_csv_path: String,

    #[arg(long)]
    exclude: String
}

// Given (a) JSONL stream of entities on stdin
// (b) metadata json with subject->metadata mappings
// and (c) summary json to tell us all possible node and edge properties
//
// This program makes gzipped NODES and EDGES csv files to import into neo4j.
// - Any property value that is the id of another entity is also written as an edge.
// - Reifications are stored as edge properties
//

fn main() -> std::io::Result<()> {

    let args = Args::parse();

    let start_time = std::time::Instant::now();

    let node_metadata = load_metadata_mapping_table::load_metadata_mapping_table(&args.in_metadata_jsonl);
    let summary:Value = serde_json::from_reader(File::open(args.in_summary_json).unwrap()).unwrap();

    let all_entity_props: Vec<String> = summary["entity_props"].as_object().unwrap().keys().cloned().collect();
    let all_edge_props: Vec<String> = summary["edge_props"].as_object().unwrap().keys().cloned().collect();

    let exclude:BTreeSet<Vec<u8>> = args.exclude.split(",").map(|s| s.to_string().as_bytes().to_vec()).collect();


    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);

    let mut nodes_file = File::create(args.out_nodes_csv_path).unwrap();
    let mut nodes_writer =
        BufWriter::with_capacity(1024*1024*32,
            &nodes_file
        );

    let mut edges_file = File::create(args.out_edges_csv_path).unwrap();
    let mut edges_writer =
        BufWriter::with_capacity(1024*1024*32,
            &edges_file
        );

    nodes_writer.write_all("grebi:nodeId:ID,:LABEL,grebi:datasources:string[]".as_bytes()).unwrap();
    for prop in &all_entity_props {
        nodes_writer.write_all(b",").unwrap();
        nodes_writer.write_all(prop.as_bytes()).unwrap();
        nodes_writer.write_all(b":string[]").unwrap();
    }
    nodes_writer.write_all(",_json:string\n".as_bytes()).unwrap();


    edges_writer.write_all(":START_ID,:TYPE,:END_ID,grebi:datasources:string[]".as_bytes()).unwrap();
    for prop in &all_edge_props {
        edges_writer.write_all(b",").unwrap();
        edges_writer.write_all(prop.as_bytes()).unwrap();
        edges_writer.write_all(b":string[]").unwrap();
    }
    edges_writer.write_all(",_json:string\n".as_bytes()).unwrap();


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

        write_node(&line, &sliced, &all_entity_props, &node_metadata, &mut nodes_writer);

        n_nodes = n_nodes + 1;
        if n_nodes % 1000000 == 0 {
            eprintln!("... written {} nodes", n_nodes);
        }

        sliced.props.iter().for_each(|prop| {
            for val in &prop.values {
                maybe_write_edge(sliced.id, prop, &val, &all_edge_props, &mut edges_writer, &exclude, &node_metadata, &val.datasources);
            }
        });
    }

    nodes_writer.flush().unwrap();
    edges_writer.flush().unwrap();

    nodes_file.sync_all().unwrap();
    edges_file.sync_all().unwrap();

    eprintln!("prepare_db_import took {} seconds", start_time.elapsed().as_secs());

    Ok(())
}

fn write_node(src_line:&[u8], entity:&SlicedEntity, all_node_props:&Vec<String>, node_metadata:&BTreeMap<Vec<u8>,Metadata>, nodes_writer:&mut BufWriter<&File>) {

    // grebi:nodeId
    nodes_writer.write_all(b"\"").unwrap();
    write_escaped_value(entity.id, nodes_writer);
    nodes_writer.write_all(b"\",\"").unwrap();

    // :LABEL
    nodes_writer.write_all(b"GraphNode").unwrap();
    entity.props.iter().for_each(|prop| {
        if prop.key == "grebi:type".as_bytes() {
            for val in &prop.values {
                nodes_writer.write_all(&[(31 as u8)]).unwrap();
                parse_json_and_write(val.value, node_metadata, nodes_writer);
            }
        }
    });

    nodes_writer.write_all(b"\",\"").unwrap();

    // grebi:datasources
    let mut is_first = true;
    entity.datasources.iter().for_each(|ds| {
        if is_first {
            is_first = false;
        } else {
            nodes_writer.write_all(&[(31 as u8)]).unwrap();
        }
        nodes_writer.write_all(ds).unwrap();
    });

    nodes_writer.write_all(b"\"").unwrap();

    for header_prop in all_node_props {
            nodes_writer.write_all(b",").unwrap();
            let mut wrote_any = false;
            for row_prop in entity.props.iter() {
                if row_prop.key == "grebi:nodeId".as_bytes() {
                    continue; // already put in first column
                }
                if row_prop.key == "grebi:type".as_bytes() {
                    continue; // already put in :LABEL column
                }
                if header_prop.as_bytes() == row_prop.key {
                    for val in row_prop.values.iter() {
                        if !wrote_any {
                            nodes_writer.write_all(b"\"").unwrap();
                            wrote_any = true;
                        } else {
                            nodes_writer.write_all(&[(31 as u8)]).unwrap();
                        }
                        if val.kind == JsonTokenType::StartObject {
                            let reified = SlicedReified::from_json(&val.value); 
                            if reified.is_some() {
                                parse_json_and_write(reified.unwrap().value, node_metadata, nodes_writer);
                                continue;
                            }
                        }
                        parse_json_and_write(val.value, node_metadata, nodes_writer);
                    }
                    continue;
                }
            }
            if wrote_any {
                nodes_writer.write_all(b"\"").unwrap();
            }
        }


    let _refs = {
        let mut res:BTreeMap<&[u8],&[u8]> = BTreeMap::new();
        for (start,end) in find_strings(&src_line) {
            let maybe_id = &src_line[start..end];
            let metadata = node_metadata.get(maybe_id);
            if metadata.is_some() {
                res.insert(maybe_id, &metadata.unwrap().json);
            }
        }
        res
    };

    nodes_writer.write_all(b",").unwrap();
    nodes_writer.write_all(b"\"").unwrap();
    write_escaped_value(&src_line[0..src_line.len()-1] /* skip closing } */, nodes_writer);
    write_escaped_value(b",\"_refs\":{", nodes_writer);

    let mut is_first_ref = true;
    for (id,md) in _refs {
        if is_first_ref {
            is_first_ref = false;
        } else {
            nodes_writer.write_all(b",").unwrap();
        }
        write_escaped_value(b"\"", nodes_writer);
        write_escaped_value(id, nodes_writer);
        write_escaped_value(b"\":", nodes_writer);
        write_escaped_value(md, nodes_writer);
    }

    nodes_writer.write_all(b"}}\"").unwrap();
    nodes_writer.write_all(b"\n").unwrap();

}

fn maybe_write_edge(from_id:&[u8], prop: &SlicedProperty, val:&SlicedPropertyValue, all_edge_props:&Vec<String>, edges_writer: &mut BufWriter<&File>, exclude:&BTreeSet<Vec<u8>>, node_metadata:&BTreeMap<Vec<u8>, Metadata>, datasources:&Vec<&[u8]>) {

    if prop.key.eq(b"id") || exclude.contains(prop.key) {
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
                    write_edge(from_id, str, prop.key, &buf,  Some(&reified_u.props), &all_edge_props,edges_writer,  node_metadata, &datasources);
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
            write_edge(from_id, str, prop.key, &buf, None, &all_edge_props, edges_writer, node_metadata, &datasources);
        }

    } else if val.kind == JsonTokenType::StartArray {

        // panic!("unexpected array, value: {:?}", String::from_utf8_lossy(prop.value));

    } else {

        // panic!("unexpected kind: {:?}", prop.kind);

    }

}

fn write_edge(from_id: &[u8], to_id: &[u8], edge:&[u8], edge_props_src_json:&[u8], edge_props:Option<&Vec<SlicedProperty>>, all_edge_props:&Vec<String>, edges_writer: &mut BufWriter<&File>, node_metadata:&BTreeMap<Vec<u8>,Metadata>, datasources:&Vec<&[u8]>) {

    edges_writer.write_all(b"\"").unwrap();
    write_escaped_value(from_id, edges_writer);
    edges_writer.write_all(b"\",\"").unwrap();
    write_escaped_value(edge, edges_writer);
    edges_writer.write_all(b"\",\"").unwrap();
    write_escaped_value(to_id, edges_writer);
    edges_writer.write_all(b"\",\"").unwrap();

    // grebi:datasources
    let mut is_first_ds = true;
    datasources.iter().for_each(|ds| {
        if is_first_ds {
            is_first_ds = false;
        } else {
            edges_writer.write_all(&[(31 as u8)]).unwrap();
        }
        edges_writer.write_all(ds).unwrap();
    });
    edges_writer.write_all(b"\"").unwrap();


    for header_prop in all_edge_props {
        edges_writer.write_all(b",").unwrap();
        if edge_props.is_some() {
            edges_writer.write_all(b"\"").unwrap();
            let mut is_first = true;
            for row_prop in edge_props.unwrap() {
                for val in row_prop.values.iter() {
                    if header_prop.as_bytes() == row_prop.key {
                        if is_first {
                            is_first = false;
                        } else {
                            edges_writer.write_all(&[(31 as u8)]).unwrap();
                        }
                        parse_json_and_write(val.value, node_metadata, edges_writer);
                        break;
                    }
                }
            }
            edges_writer.write_all(b"\"").unwrap();
        }
    }

    edges_writer.write_all(b",").unwrap();
    edges_writer.write_all(b"\"").unwrap();
    write_escaped_value(edge_props_src_json, edges_writer);
    edges_writer.write_all(b"\"").unwrap();

    edges_writer.write_all(b"\n").unwrap();

}

fn write_escaped_value(buf:&[u8], writer:&mut BufWriter<&File>) {

    for byte in buf.iter() {
        match byte {
            b'\n' => writer.write_all(b"\\n").unwrap(),
            b'\r' => writer.write_all(b"\\r").unwrap(),
            b'\t' => writer.write_all(b"\\t").unwrap(),
            b'\\' => writer.write_all(b"\\\\").unwrap(),
            b'"' => writer.write_all(b"\"\"").unwrap(),
            b => writer.write_all(&[*b]).unwrap(),
        }
    }
}


fn parse_json_and_write(buf:&[u8], node_metadata:&BTreeMap<Vec<u8>,Metadata>, writer:&mut BufWriter<&File>) {

    let mut json = JsonParser::parse(buf);

    match json.peek().kind {
        JsonTokenType::StartString => {
            let str = json.string();
            write_escaped_value(str, writer);
            let metadata = node_metadata.get(str);
            if metadata.is_some() {
                let metadata_u = metadata.unwrap();
                if metadata_u.name.is_some() {
                    let name = metadata_u.name.as_ref().unwrap();
                    writer.write_all(&[(31 as u8)]).unwrap();
                    write_escaped_value(&name, writer);
                }
            }
        },
        _ => {
            write_escaped_value(&buf, writer)
        }
    }
}

