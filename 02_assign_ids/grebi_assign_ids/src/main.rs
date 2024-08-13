


use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::{env, io};
use std::io::{BufRead, BufReader };
use std::io::{Write, BufWriter};
use grebi_shared::json_lexer::{lex, JsonTokenType};
use grebi_shared::json_parser::JsonParser;
use clap::Parser;

use grebi_shared::find_strings;
use grebi_shared::load_groups_txt::load_groups_txt;


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

    let id_to_group:HashMap<Vec<u8>, Vec<u8>> = load_groups_txt(&args.groups_txt);

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

        let mut id:Option<&[u8]> = None;

        json.begin_object();
        json.mark();
        while json.peek().kind != JsonTokenType::EndObject {
            let prop_key = json.name();

            // any of the IDs will do, we only need one
            // as all identifiers map to the same group
            //
            if id_props.contains(prop_key) {
		// TODO handle the same cases as the id extraction does
		if json.peek().kind == JsonTokenType::StartArray {
			json.begin_array();
			id = Some(json.string());
		} else {
			id = Some(json.string());
		}
		break;
            } else {
                json.value(); // skip
            }
        }

        writer.write_all("{\"grebi:nodeId\":\"".as_bytes()).unwrap();

        let group = id_to_group.get(id.unwrap());
        if group.is_some() {
            writer.write_all(group.unwrap().as_slice()).unwrap();
        } else {
            writer.write_all(id.unwrap()).unwrap();
        }

        writer.write_all("\"".as_bytes()).unwrap();


        json.rewind();
        while json.peek().kind != JsonTokenType::EndObject {

            writer.write_all(b",\"").unwrap();

            let name = json.name();
            if name.eq(b"id") {
                writer.write_all(b"id").unwrap();
            } else {
                let name_group = id_to_group.get(name);
                if name_group.is_some() {
                    writer.write_all(name_group.unwrap()).unwrap();
                } else {
                    writer.write_all(name).unwrap();
                }
            }
            writer.write_all(b"\":").unwrap();

            if name.eq(b"id") || preserve_fields.contains(name) {
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
            writer.write_all(pv_group.unwrap()).unwrap();
        } else {
            writer.write_all(str).unwrap();
        }

        n_ch = cur_end;
        n_found_str = n_found_str + 1;
    }

    writer.write_all(&value[n_ch..value.len()]).unwrap();
}
