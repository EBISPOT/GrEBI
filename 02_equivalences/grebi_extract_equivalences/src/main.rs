


use std::collections::{HashSet, HashMap};
use std::ops::Deref;
use std::{env, io};
use rocksdb::{DB, Options, BlockBasedOptions};
use std::io::{BufRead, BufReader };
use std::io::{Write, BufWriter};

use grebi_shared::{get_subject, find_strings, serialize_equivalence, json_parser, json_lexer};

use clap::Parser;

use grebi_shared::json_lexer::{lex, JsonTokenType};
use grebi_shared::json_parser::JsonParser;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    equivalence_properties:String
}

fn main() {

    let start_time = std::time::Instant::now();

    let stdin = io::stdin();
    let handle = stdin.lock();
    let mut reader = BufReader::new(handle);

    let stdout = io::stdout().lock();
    let mut writer = BufWriter::new(stdout);

    let mut equiv_props:HashSet<Vec<u8>> = HashSet::new();

    let mut n_total = 0;

    let args = Args::parse();
    for prop in args.equivalence_properties.split(",") {
        equiv_props.insert(prop.as_bytes().to_vec());
    }

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
            let name = json.name();
            if name.eq("id".as_bytes()) {
                id = Some(json.string());
                break;
            } else {
                json.value(); // skip
            }
        }
        json.rewind();

        if id.is_none() {
            panic!("Missing id field in JSON: {}", String::from_utf8(line).unwrap());
        }

        while json.peek().kind != JsonTokenType::EndObject {

            let k = json.name();

            if !equiv_props.contains(k) {
                json.value(); // skip
                continue;
            }

            if json.peek().kind == JsonTokenType::StartArray {
                json.begin_array();
                while json.peek().kind != JsonTokenType::EndArray {
                    if json.peek().kind == JsonTokenType::StartString {
                        let serialized = serialize_equivalence(id.unwrap(), json.string());
                        if serialized.is_some() {
                            writer.write_all(&serialized.unwrap()).unwrap();
                        }
                    } else {
                        json.value(); // skip
                    }
                }
                json.end_array();
            } else if json.peek().kind == JsonTokenType::StartString {
                let serialized = serialize_equivalence(id.unwrap(), json.string());
                if serialized.is_some() {
                    writer.write_all(&serialized.unwrap()).unwrap();
                }
            } else {
                json.value(); // skip
            }
        }

        n_total = n_total + 1;

        if n_total % 1000000 == 0 {
            eprintln!("processed {} objects in {} seconds", n_total, start_time.elapsed().as_secs());
        }
    }

    writer.flush().unwrap();

}



