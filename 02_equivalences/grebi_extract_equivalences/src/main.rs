


use std::collections::{HashSet, HashMap};
use std::ops::Deref;
use std::{env, io};
use rocksdb::{DB, Options, BlockBasedOptions};
use std::io::{BufRead, BufReader };
use std::io::{Write, BufWriter};

use grebi_shared::{get_subject, find_strings, serialize_equivalence, json_parser, json_lexer};

mod slice_entity;
use slice_entity::SlicedEntity;
use slice_entity::SlicedProperty;
use clap::Parser;

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
        
        // eprintln!("line: {}", String::from_utf8(line.clone()).unwrap());

        let sliced = SlicedEntity::from_json(&line);

        let subject:&[u8] = sliced.subject;

        for prop in sliced.props.iter() {
            if equiv_props.contains(prop.key) {
                for v in prop.values.iter() {

                    let mut json = json_parser::JsonParser::from_lexed(json_lexer::lex(v));
                    if json.peek().kind == json_lexer::JsonTokenType::StartString {
                        let serialized = serialize_equivalence(subject, json.string(v));
                        if serialized.is_some() {
                            writer.write_all(&serialized.unwrap()).unwrap();
                        }
                    }

                }
            }
        }

        n_total = n_total + 1;

        if n_total % 1000000 == 0 {
            eprintln!("scanned {} subjects in {} seconds", n_total, start_time.elapsed().as_secs());
        }
    }

    writer.flush().unwrap();

}



