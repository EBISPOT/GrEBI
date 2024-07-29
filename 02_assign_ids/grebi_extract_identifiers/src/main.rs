


use std::collections::{HashSet, HashMap};
use std::ops::Deref;
use std::{env, io};
use std::io::{BufRead, BufReader };
use std::io::{Write, BufWriter};

use grebi_shared::{get_subject, find_strings, json_parser, json_lexer};

use clap::Parser;

use grebi_shared::json_lexer::{lex, JsonTokenType};
use grebi_shared::json_parser::JsonParser;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    identifier_properties:String
}

fn main() {

    let start_time = std::time::Instant::now();

    let stdin = io::stdin();
    let handle = stdin.lock();
    let mut reader = BufReader::new(handle);

    let stdout = io::stdout().lock();
    let mut writer = BufWriter::new(stdout);

    let mut id_props:HashSet<Vec<u8>> = HashSet::new();

    let mut n_total = 0;

    let args = Args::parse();
    for prop in args.identifier_properties.split(",") {
        id_props.insert(prop.as_bytes().to_vec());
    }

    loop {
        let mut line: Vec<u8> = Vec::new();
        reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            break;
        }

        let mut json = JsonParser::parse(&line);
        json.begin_object();

        let mut wrote_any = false;

        while json.peek().kind != JsonTokenType::EndObject {

            let k = json.name();

            if !id_props.contains(k) {
                json.value(); // skip
                continue;
            }

            if json.peek().kind == JsonTokenType::StartArray {
                json.begin_array();
                while json.peek().kind != JsonTokenType::EndArray {
                    if json.peek().kind == JsonTokenType::StartString {
                        if wrote_any {
                            writer.write_all(b"\t").unwrap();
                        } else {
                            wrote_any = true;
                        }
                        writer.write_all(&json.string()).unwrap();
                    } else {
                        json.value(); // skip
                    }
                }
                json.end_array();
            } else if json.peek().kind == JsonTokenType::StartString {
                if wrote_any {
                    writer.write_all(b"\t").unwrap();
                } else {
                    wrote_any = true;
                }
                writer.write_all(&json.string()).unwrap();
            } else {
                json.value(); // skip
            }
        }
        if !wrote_any {
            panic!("no identifiers found in object {}", String::from_utf8_lossy(&line));
        }

        writer.write_all(b"\n").unwrap();

        n_total = n_total + 1;

        if n_total % 1000000 == 0 {
            eprintln!("processed {} objects in {} seconds", n_total, start_time.elapsed().as_secs());
        }
    }

    writer.flush().unwrap();

}



