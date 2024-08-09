

use std::fs::File;
use std::io::BufWriter;
use std::io::BufReader;
use std::io::Write;
use std::io::BufRead;
use std::collections::HashSet;
use clap::Parser;
use grebi_shared::json_lexer::JsonTokenType;
use grebi_shared::slice_materialised_edge::SlicedEdge;
use serde_json::Map;
use serde_json::Value;
use grebi_shared::slice_merged_entity::SlicedEntity;
use grebi_shared::slice_merged_entity::SlicedReified;
use grebi_shared::slice_merged_entity::SlicedPropertyValue;
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
    in_summary_jsons: String,

    #[arg(long)]
    out_nodes_csv_path: String,

    #[arg(long)]
    out_edges_csv_path: String,

    #[arg(long)]
    out_id_edges_csv_path: String,

    #[arg(long)]
    add_prefix: String, // used to prepend the subgraph name like hra_kg:g:
}

fn main() -> std::io::Result<()> {

    let args = Args::parse();

    let add_prefix = args.add_prefix.as_bytes();

    let start_time = std::time::Instant::now();


    let mut all_entity_props: HashSet<String> = HashSet::new();
    let mut all_edge_props: HashSet<String> = HashSet::new();


    for f in args.in_summary_jsons.split(",") {
        let summary:Value = serde_json::from_reader(File::open(f).unwrap()).unwrap();
        for prop in summary["edge_props"].as_object().unwrap().keys() {
            all_edge_props.insert(prop.to_string());
        }
        for prop in summary["entity_props"].as_object().unwrap().keys() {
            all_entity_props.insert(prop.to_string());
        }
    }


    let mut nodes_reader = BufReader::new(File::open(args.in_nodes_jsonl).unwrap());
    let mut edges_reader = BufReader::new(File::open(args.in_edges_jsonl).unwrap());

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

    let mut id_edges_file = File::create(args.out_id_edges_csv_path).unwrap();
    let mut id_edges_writer =
        BufWriter::with_capacity(1024*1024*32,
            &id_edges_file
        );



    nodes_writer.write_all("grebi:nodeId:ID,:LABEL,grebi:datasources:string[],grebi:subgraph:string".as_bytes()).unwrap();
    for prop in &all_entity_props {
        nodes_writer.write_all(b",").unwrap();
        nodes_writer.write_all(prop.as_bytes()).unwrap();
        nodes_writer.write_all(b":string[]").unwrap();
    }
    nodes_writer.write_all("\n".as_bytes()).unwrap();


    edges_writer.write_all(":START_ID,:TYPE,:END_ID,edge_id:string,grebi:datasources:string[],grebi:subgraph:string".as_bytes()).unwrap();
    for prop in &all_edge_props {
        edges_writer.write_all(b",").unwrap();
        edges_writer.write_all(prop.as_bytes()).unwrap();
        edges_writer.write_all(b":string[]").unwrap();
    }
    edges_writer.write_all("\n".as_bytes()).unwrap();


    id_edges_writer.write_all(":START_ID,:TYPE,:END_ID\n".as_bytes()).unwrap();


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

        let sliced = SlicedEntity::from_json(&line);

        write_node(&line, &sliced, &all_entity_props, &mut nodes_writer, &mut id_edges_writer, &add_prefix);

        n_nodes = n_nodes + 1;
        if n_nodes % 1000000 == 0 {
            eprintln!("... written {} nodes", n_nodes);
        }
    }

    let mut n_edges:i64 = 0;

    loop {
        let mut line: Vec<u8> = Vec::new();
        edges_reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            break;
        }
        if line[line.len() - 1] == b'\n' {
            line.pop();
        }

        let sliced = SlicedEdge::from_json(&line);

        write_edge(&line, sliced, &all_edge_props, &mut edges_writer, &add_prefix);

        n_edges = n_edges + 1;
        if n_edges % 1000000 == 0 {
            eprintln!("... written {} edges", n_edges);
        }
    }

    nodes_writer.flush().unwrap();
    edges_writer.flush().unwrap();

    nodes_file.sync_all().unwrap();
    edges_file.sync_all().unwrap();

    eprintln!("prepare_db_import took {} seconds", start_time.elapsed().as_secs());

    Ok(())
}

fn write_node(src_line:&[u8], entity:&SlicedEntity, all_node_props:&HashSet<String>, nodes_writer:&mut BufWriter<&File>, id_edges_writer:&mut BufWriter<&File>, add_prefix:&[u8]) {

    let refs:Map<String,Value> = serde_json::from_slice(entity._refs.unwrap()).unwrap();

    // grebi:nodeId
    nodes_writer.write_all(b"\"").unwrap();
    nodes_writer.write_all(&add_prefix).unwrap();
    write_escaped_value(entity.id, nodes_writer);
    nodes_writer.write_all(b"\",\"").unwrap();

    // :LABEL
    nodes_writer.write_all(b"GraphNode").unwrap();
    entity.props.iter().for_each(|prop| {
        if prop.key == "grebi:type".as_bytes() {
            for val in &prop.values {
                nodes_writer.write_all(&[(31 as u8)]).unwrap();
                parse_json_and_write(val.value, &refs, nodes_writer);
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

    nodes_writer.write_all(b"\",\"").unwrap();
    nodes_writer.write_all(entity.subgraph).unwrap();
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
                    if row_prop.key == "id".as_bytes() {
                        for val in row_prop.values.iter() {
                            write_id_row(val, id_edges_writer, &entity.id, &add_prefix);
                        }
                    }
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
                                parse_json_and_write(reified.unwrap().value, &refs, nodes_writer);
                                continue;
                            }
                        }
                        parse_json_and_write(val.value, &refs, nodes_writer);
                    }
                    continue;
                }
            }
            if wrote_any {
                nodes_writer.write_all(b"\"").unwrap();
            }
        }


    nodes_writer.write_all(b"\n").unwrap();
}

fn write_edge(src_line:&[u8], edge:SlicedEdge, all_edge_props:&HashSet<String>, edges_writer: &mut BufWriter<&File>, add_prefix:&[u8]) {

    let refs:Map<String,Value> = serde_json::from_slice(edge._refs.unwrap()).unwrap();

    edges_writer.write_all(b"\"").unwrap();
    write_escaped_value(&add_prefix, edges_writer);
    write_escaped_value(edge.from, edges_writer);
    edges_writer.write_all(b"\",\"").unwrap();
    write_escaped_value(edge.edge_type, edges_writer);
    edges_writer.write_all(b"\",\"").unwrap();
    write_escaped_value(&add_prefix, edges_writer);
    write_escaped_value(edge.to, edges_writer);
    edges_writer.write_all(b"\",\"").unwrap();
    write_escaped_value(&add_prefix, edges_writer);
    write_escaped_value(edge.edge_id, edges_writer);
    edges_writer.write_all(b"\",\"").unwrap();

    // grebi:datasources
    let mut is_first_ds = true;
    edge.datasources.iter().for_each(|ds| {
        if is_first_ds {
            is_first_ds = false;
        } else {
            edges_writer.write_all(&[(31 as u8)]).unwrap();
        }
        edges_writer.write_all(ds).unwrap();
    });

    edges_writer.write_all(b"\",\"").unwrap();
    edges_writer.write_all(edge.subgraph).unwrap();
    edges_writer.write_all(b"\"").unwrap();


    for header_prop in all_edge_props {
        edges_writer.write_all(b",").unwrap();
        edges_writer.write_all(b"\"").unwrap();
        let mut is_first = true;
        for row_prop in &edge.props {
            for val in row_prop.values.iter() {
                if header_prop.as_bytes() == row_prop.key {
                    if is_first {
                        is_first = false;
                    } else {
                        edges_writer.write_all(&[(31 as u8)]).unwrap();
                    }
                    parse_json_and_write(val.value, &refs, edges_writer);
                    break;
                }
            }
        }
        edges_writer.write_all(b"\"").unwrap();
    }

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


fn parse_json_and_write(buf:&[u8], refs:&Map<String,Value>, writer:&mut BufWriter<&File>) {

    let mut json = JsonParser::parse(buf);

    match json.peek().kind {
        JsonTokenType::StartString => {
            let str = json.string();
            write_escaped_value(str, writer);
            let metadata = refs.get(&String::from_utf8_lossy(str).to_string());
            if metadata.is_some() {
                let metadata_u = metadata.unwrap();
                let names = metadata_u.get("grebi:name");
                if names.is_some() {
                    for name in names.unwrap().as_array().unwrap() {
                        writer.write_all(&[(31 as u8)]).unwrap();
                        write_escaped_value(name.as_str().unwrap().as_bytes(), writer);
                    }
                }
            }
        },
        _ => {
            write_escaped_value(&buf, writer)
        }
    }
}

fn write_id_row(val:&SlicedPropertyValue, id_edges_writer:&mut BufWriter<&File>, grebi_node_id:&[u8], add_prefix:&[u8]) {

    let actual_id = {
        if val.kind == JsonTokenType::StartObject {
            let reified = SlicedReified::from_json(&val.value); 
            if reified.is_some() {
                JsonParser::parse(&reified.unwrap().value).string()
            } else {
                JsonParser::parse(&val.value).string()
            }
        } else { 
            JsonParser::parse(&val.value).string()
        }
    };

    id_edges_writer.write_all(b"\"").unwrap();
    write_escaped_value(&add_prefix, id_edges_writer);
    write_escaped_value(grebi_node_id, id_edges_writer);
    id_edges_writer.write_all(b"\",\"").unwrap();
    write_escaped_value(b"id", id_edges_writer);
    id_edges_writer.write_all(b"\",\"").unwrap();
    write_escaped_value(actual_id, id_edges_writer);
    id_edges_writer.write_all(b"\"\n").unwrap();
}

