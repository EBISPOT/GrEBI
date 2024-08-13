



use std::collections::{BTreeSet, HashMap, HashSet};
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
    groups_txt:String,

    #[arg(long)]
    type_superclasses:String,

}

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() {

    let args = Args::parse();

    let mut type_superclasses:HashSet<Vec<u8>> = {
        let id_to_group:HashMap<Vec<u8>, Vec<u8>> = load_groups_txt(args.groups_txt.as_str());
        let mut res = HashSet::new();
        for prop in args.type_superclasses.split(",") {
            let mapped = id_to_group.get(prop.as_bytes());
            if mapped.is_some() {
                res.insert(mapped.unwrap().to_vec());
            } else {
                res.insert(prop.as_bytes().to_vec());
            }
        }
        res
    };

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
        
        let mut json = JsonParser::parse(&line);

        let mut id:Option<&[u8]> = None;
        let mut types:BTreeSet<&[u8]> = BTreeSet::new();

        json.begin_object();
        json.mark();
        while json.peek().kind != JsonTokenType::EndObject {
            let prop_key = json.name();

            if prop_key.eq(b"grebi:type") {
                if json.peek().kind == JsonTokenType::StartArray {
                    json.begin_array();
                    while json.peek().kind != JsonTokenType::EndArray {
                        types.insert(json.string());
                    }
                    json.end_array();
                } else {
                    types.insert(json.string());
                }
            } else if prop_key.eq(b"ols:directAncestor") {
                if json.peek().kind == JsonTokenType::StartArray {
                    json.begin_array();
                    while json.peek().kind != JsonTokenType::EndArray {
                        let ancestor = json.string();
                        if type_superclasses.contains(ancestor) {
                            types.insert(ancestor);
                        }
                    }
                    json.end_array();
                } else {
                    let ancestor = json.string();
                    if type_superclasses.contains(ancestor) {
                        types.insert(ancestor);
                    }
                }
            } else {
                json.value(); // skip
            }
        }

        json.rewind();

        writer.write_all(b"{").unwrap();

        let mut is_first = true;

        while json.peek().kind != JsonTokenType::EndObject {
            if is_first {
                is_first = false;
            } else {
                writer.write_all(b",").unwrap();
            }

            let name = json.name();

            writer.write_all(b"\"").unwrap();
            writer.write_all(name).unwrap();
            writer.write_all(b"\":").unwrap();

            if name.eq(b"grebi:type") {
                json.value(); // skip, we already have the types

                writer.write_all(b"[").unwrap();
                let mut is_first_type = true;
                for t in types.iter() {
                    if is_first_type {
                        is_first_type = false;
                    } else {
                        writer.write_all(b",").unwrap();
                    }
                    writer.write_all(b"\"").unwrap();
                    writer.write_all(t).unwrap();
                    writer.write_all(b"\"").unwrap();
                }
                writer.write_all(b"]").unwrap();
            } else {
                writer.write_all(json.value()).unwrap();
            }
        }

        writer.write_all(b"}\n").unwrap();
    }

    eprintln!("completed superclass2types in {}", start_time.elapsed().as_secs());

}
