


use std::collections::{HashMap, HashSet, BTreeSet};
use std::fs::File;
use std::{env, io};
use std::io::{BufRead, BufReader };
use std::io::{Write, BufWriter};
use grebi_shared::json_lexer::{lex, JsonTokenType};
use grebi_shared::json_parser::JsonParser;
use clap::Parser;

use grebi_shared::find_strings;
use grebi_shared::load_groups_txt::load_id_to_group_mapping;
use grebi_shared::check_id;


#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    identifier_properties:String,

    #[arg(long)]
    groups_txt: String,
    
    #[arg(long)]
    preserve_field: Vec<String>

}

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() {

    let args = Args::parse();


    let mut id_props:HashSet<Vec<u8>> = HashSet::new();
    for prop in args.identifier_properties.split(",") {
        id_props.insert(prop.as_bytes().to_vec());
    }


    let preserve_fields:HashSet<Vec<u8>> = args.preserve_field.iter().map(|x| x.as_bytes().to_vec()).collect();

    let id_to_group = load_id_to_group_mapping(&args.groups_txt);

    let start_time = std::time::Instant::now();

    let stdin = io::stdin();
    let handle = stdin.lock();
    let mut reader = BufReader::new(handle);

    let stdout = io::stdout().lock();
    // let mut writer = BufWriter::new(stdout);
    let mut writer = BufWriter::new(stdout);

    loop {
        let mut line: Vec<u8> = Vec::new();
        reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            break;
        }
        
        let mut json = JsonParser::parse(&line);

        let mut ids:BTreeSet<&[u8]> = BTreeSet::new();

        json.begin_object();
        json.mark();
        while json.peek().kind != JsonTokenType::EndObject {

            let k = json.name();

            if !id_props.contains(k) {
                json.value(); // skip
                continue;
            }

            get_ids(&mut json, &mut ids);
        }

        if ids.len() == 0 {
            eprintln!("!!! skipping object with no identifiers: {}", String::from_utf8_lossy(&line));
            continue;
        }

        writer.write_all("{\"grebi:nodeId\":\"".as_bytes()).unwrap();

        // just get the first id, doesn't matter bc all map to the same group 
        let id = ids.iter().next().unwrap();
        let group = id_to_group.get(id.clone());
        if group.is_some() {
            writer.write_all(group.unwrap().as_slice()).unwrap();
        } else {
            writer.write_all(id).unwrap();
        }

        writer.write_all("\"".as_bytes()).unwrap();
        writer.write_all(",\"grebi:sourceIds\":[".as_bytes()).unwrap();
        let mut is_first_id = true;
        for &id in ids.iter() {
            if is_first_id {
                is_first_id = false;
            } else {
                writer.write_all(b",").unwrap();
            }
            writer.write_all(b"\"").unwrap();
            writer.write_all(id).unwrap();
            writer.write_all(b"\"").unwrap();
        }
        writer.write_all("]".as_bytes()).unwrap();

        json.rewind();
        while json.peek().kind != JsonTokenType::EndObject {

            let name = json.name();
            let name_group = id_to_group.get(name);
            if name_group.is_some() {
                writer.write_all(b"mapped##").unwrap();
                writer.write_all(name).unwrap();
                writer.write_all(b"##").unwrap();
                writer.write_all(name_group.unwrap()).unwrap();
            } else {
                writer.write_all(name).unwrap();
            }
            writer.write_all(b"\":").unwrap();

            if preserve_fields.contains(name) {
                writer.write_all(json.value()).unwrap();
            } else {
                write_value(&mut writer, json.value(), &id_to_group);
            }
        }

        writer.write_all(b"}\n").unwrap();
    }

    eprintln!("completed id to group mapping in {}", start_time.elapsed().as_secs());

}

fn write_value(writer:&mut BufWriter<io::StdoutLock>, value:&[u8], id_to_group:&HashMap<Vec<u8>, Vec<u8>>) {

    let string_locations = find_strings(&value);

    if string_locations.len() == 0 {
        // no strings in the json, just write it as is
        writer.write_all(&value).unwrap();
        return;
    }

    // replace any strings in the json that are IDs with their group IDs (if extant)

    let mut n_ch = 0;
    let mut n_found_str = 0;

    while n_found_str < string_locations.len() {
        let (cur_start, cur_end) = string_locations[n_found_str];
        let str = &value[cur_start..cur_end];

        writer.write_all(&value[n_ch..cur_start]).unwrap();

        let pv_group = id_to_group.get(str);
        if pv_group.is_some() {
            writer.write_all(b"mapped##").unwrap();
            writer.write_all(str).unwrap();
            writer.write_all(b"##").unwrap();
            writer.write_all(pv_group.unwrap()).unwrap();
        } else {
            writer.write_all(str).unwrap();
        }

        n_ch = cur_end;
        n_found_str = n_found_str + 1;
    }

    writer.write_all(&value[n_ch..value.len()]).unwrap();
}

fn write_escaped_string(str:&[u8], writer:&mut BufWriter<io::StdoutLock>) {
    for c in str {
        match c {
            b'"' => { writer.write_all(b"\\\"").unwrap(); }
            b'\\' => { writer.write_all(b"\\\\").unwrap(); }
            b'\n' => { writer.write_all(b"\\n").unwrap(); }
            b'\r' => { writer.write_all(b"\\r").unwrap(); }
            b'\t' => { writer.write_all(b"\\t").unwrap(); }
            _ => { writer.write_all([*c].as_slice()).unwrap(); }
        }
    }
}

fn get_ids<'a, 'b>(json:&mut JsonParser<'a>, ids:&'b mut BTreeSet<&'a [u8]>) {

    if json.peek().kind == JsonTokenType::StartArray {
        json.begin_array();
        while json.peek().kind != JsonTokenType::EndArray {
            get_ids(json, ids);
        }
        json.end_array();
    } else if json.peek().kind == JsonTokenType::StartString {
        let id = json.string();
        ids.insert(id.clone());
    } else if json.peek().kind == JsonTokenType::StartObject {
        // maybe a reification
        json.begin_object();
        while json.peek().kind != JsonTokenType::EndObject {
            let k = json.name();
            if k.eq(b"grebi:value") {
                get_ids(json, ids);
            } else {
                json.value(); // skip
            }
        }
        json.end_object();
    } else {
        json.value(); // skip
    }
}

