

use std::ascii::escape_default;
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
use grebi_shared::json_lexer::JsonTokenType;
use grebi_shared::json_parser;
use grebi_shared::prefix_map::PrefixMap;

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
    in_subjects_txt: String,

    #[arg(long)]
    in_metadata_json_path: String,

    #[arg(long)]
    out_nodes_csv_path: String,

    #[arg(long)]
    out_edges_csv_path: String,

    #[arg(long)]
    exclude: String
}

// Given (a) JSONL stream of entities on stdin
// (b) subjects.txt with all possible subjects
// and (c) metadata json to tell us all possible node and edge properties
//
// This program makes gzipped NODES and EDGES csv files to import into neo4j.
// - Any property value that is the id of another entity is also written as an edge.
// - Reifications are stored as edge properties
//

fn main() -> std::io::Result<()> {

    let args = Args::parse();

    let start_time = std::time::Instant::now();

    let all_subjects:BTreeSet<Vec<u8>> = {
        let start_time = std::time::Instant::now();
        let mut res:BTreeSet<Vec<u8>> = BTreeSet::new();
        let mut reader = BufReader::new(File::open(&args.in_subjects_txt).unwrap());
        loop {
            let mut line: Vec<u8> = Vec::new();
            reader.read_until(b'\n', &mut line).unwrap();
            if line.len() == 0 {
                break;
            }
            if line[line.len() - 1] == b'\n' {
                line.pop();
            }
            res.insert(line);
        }
        eprintln!("loaded {} subjects in {} seconds", res.len(), start_time.elapsed().as_secs());
        res
    };

    let index_metadata:Value = serde_json::from_reader(File::open(args.in_metadata_json_path).unwrap()).unwrap();

    let all_entity_props: Vec<String> = index_metadata["entity_props"].as_object().unwrap().keys().cloned().collect();
    let all_edge_props: Vec<String> = index_metadata["edge_props"].as_object().unwrap().keys().cloned().collect();

    let exclude:BTreeSet<Vec<u8>> = args.exclude.split(",").map(|s| s.to_string().as_bytes().to_vec()).collect();


    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);

    let mut nodes_writer =
        BufWriter::with_capacity(1024*1024*32,
            // GzEncoder::new(
            File::create(args.out_nodes_csv_path).unwrap()
            // Compression::fast())
        );

    let mut edges_writer =
        BufWriter::with_capacity(1024*1024*32,
            // GzEncoder::new(
            File::create(args.out_edges_csv_path).unwrap()
            // Compression::fast())
        );

    nodes_writer.write_all("grebi:id:ID,:LABEL,grebi:datasources:string[]".as_bytes()).unwrap();
    for prop in &all_entity_props {
        nodes_writer.write_all(b",").unwrap();
        nodes_writer.write_all(prop.as_bytes()).unwrap();
        nodes_writer.write_all(b":string[]").unwrap();
    }
    nodes_writer.write_all("\n".as_bytes()).unwrap();


    edges_writer.write_all(":START_ID,:TYPE,:END_ID,grebi:datasources:string[]".as_bytes()).unwrap();
    for prop in &all_edge_props {
        edges_writer.write_all(b",").unwrap();
        edges_writer.write_all(prop.as_bytes()).unwrap();
        edges_writer.write_all(b":string[]").unwrap();
    }
    edges_writer.write_all("\n".as_bytes()).unwrap();


    let mut n_nodes:i64 = 0;

    loop {
        let mut line: Vec<u8> = Vec::new();
        reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            break;
        }

        let sliced = SlicedEntity::from_json(&line);

        write_node(&sliced, &all_entity_props, &mut nodes_writer);

        n_nodes = n_nodes + 1;
        if n_nodes % 1000000 == 0 {
            eprintln!("... written {} nodes", n_nodes);
        }

        sliced.props.iter().for_each(|prop| {
            maybe_write_edge(sliced.id, prop, &all_subjects, &all_edge_props, &mut edges_writer, &exclude, &prop.datasources);
        });
    }

    nodes_writer.flush().unwrap();
    edges_writer.flush().unwrap();

    eprintln!("materialize_edges took {} seconds", start_time.elapsed().as_secs());

    Ok(())
}

fn write_node(entity:&SlicedEntity, all_node_props:&Vec<String>, nodes_writer:&mut BufWriter<File>) {

    // grebi:id
    nodes_writer.write_all(b"\"").unwrap();
    write_escaped_value(entity.id, nodes_writer);
    nodes_writer.write_all(b"\",\"").unwrap();

    // :LABEL
    nodes_writer.write_all(b"GraphNode").unwrap();
    entity.props.iter().for_each(|prop| {
        if prop.key == "grebi:type".as_bytes() {
            nodes_writer.write_all(b";").unwrap();
            parse_json_and_write(prop.value, nodes_writer);
        }
    });

    nodes_writer.write_all(b"\",\"").unwrap();

    // grebi:datasources
    let mut is_first = true;
    entity.datasources.iter().for_each(|ds| {
        if is_first {
            is_first = false;
        } else {
            nodes_writer.write_all(b";").unwrap();
        }
        nodes_writer.write_all(ds).unwrap();
    });

    nodes_writer.write_all(b"\"").unwrap();

    for header_prop in all_node_props {
            nodes_writer.write_all(b",").unwrap();
            let mut wrote_any = false;
            for row_prop in entity.props.iter() {
                if row_prop.key == "grebi:id".as_bytes() {
                    continue; // already put in first column
                }
                if row_prop.key == "grebi:type".as_bytes() {
                    continue; // already put in :LABEL column
                }
                if header_prop.as_bytes() == row_prop.key {
                    if !wrote_any {
                        nodes_writer.write_all(b"\"").unwrap();
                        wrote_any = true;
                    } else {
                        nodes_writer.write_all(b";").unwrap();
                    }
                    if row_prop.kind == JsonTokenType::StartObject {
                        let pv = row_prop.value.to_vec();
                        let reified = SlicedReified::from_json(&pv); // TODO make this accept a slice to avoid a copy
                        if reified.is_some() {
                            parse_json_and_write(reified.unwrap().value, nodes_writer);
                            break;
                        }
                    }
                    parse_json_and_write(row_prop.value, nodes_writer);
                    break;
                }
            }
            if wrote_any {
                nodes_writer.write_all(b"\"").unwrap();
            }
        }

    nodes_writer.write_all(b"\n").unwrap();

}

fn maybe_write_edge(from_id:&[u8], prop: &SlicedProperty, all_subjects:&BTreeSet<Vec<u8>>, all_edge_props:&Vec<String>, edges_writer: &mut BufWriter<File>, exclude:&BTreeSet<Vec<u8>>, datasources:&Vec<&[u8]>) {

    if prop.key.eq(b"id") || exclude.contains(prop.key) {
        return;
    }

    if prop.kind == JsonTokenType::StartObject {

        let value = prop.value.to_vec();

        let reified = SlicedReified::from_json(&value);

        if reified.is_some() {
            let reified_u = reified.unwrap();
            if reified_u.value_kind == JsonTokenType::StartString {
                let buf = &reified_u.value.to_vec();
                let str = JsonParser::parse(&buf).string();
                let exists = all_subjects.contains(str);
                if exists {
                    write_edge(from_id, str, prop.key,  Some(&reified_u.props), &all_edge_props, all_subjects, edges_writer, &datasources);
                }
            } else {
                // panic!("unexpected kind: {:?}", reified_u.value_kind);
            }
        } 
 
    } else if prop.kind == JsonTokenType::StartString {

        let buf = &prop.value.to_vec();
        let str = JsonParser::parse(&buf).string();
        let exists = all_subjects.contains(str);

        if exists {
            write_edge(from_id, str, prop.key, None, &all_edge_props, all_subjects, edges_writer, &datasources);
        }

    } else if prop.kind == JsonTokenType::StartArray {

        // panic!("unexpected array, value: {:?}", String::from_utf8_lossy(prop.value));

    } else {

        // panic!("unexpected kind: {:?}", prop.kind);

    }

}

fn write_edge(from_id: &[u8], to_id: &[u8], edge:&[u8], edge_props:Option<&Vec<SlicedProperty>>, all_edge_props:&Vec<String>, all_subjects:&BTreeSet<Vec<u8>>, edges_writer: &mut BufWriter<File>, datasources:&Vec<&[u8]>) {

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
            edges_writer.write_all(b";").unwrap();
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
                if header_prop.as_bytes() == row_prop.key {
                    if is_first {
                        is_first = false;
                    } else {
                        edges_writer.write_all(b";").unwrap();
                    }
                    parse_json_and_write(row_prop.value, edges_writer);
                    break;
                }
            }
            edges_writer.write_all(b"\"").unwrap();
        }
    }

    edges_writer.write_all(b"\n").unwrap();

}

fn write_escaped_value(buf:&[u8], writer:&mut BufWriter<File>) {

    for byte in buf.iter() {
        match byte {
            b'\n' => writer.write_all(b"\\n").unwrap(),
            b'\r' => writer.write_all(b"\\r").unwrap(),
            b'\t' => writer.write_all(b"\\t").unwrap(),
            b';' => writer.write_all(b"\\;").unwrap(),
            b'\\' => writer.write_all(b"\\\\").unwrap(),
            b'"' => writer.write_all(b"\"\"").unwrap(),
            b => writer.write_all(&[*b]).unwrap(),
        }
    }
}


fn parse_json_and_write(buf:&[u8], writer:&mut BufWriter<File>) {

    let v = buf.to_vec(); // TODO fix lex to accept a slice
    let mut json = JsonParser::parse(&v);

    match json.peek().kind {
        JsonTokenType::StartString => {
            write_escaped_value(json.string(), writer);
        },
        _ => {
            write_escaped_value(&buf, writer)
        }
    }
}

