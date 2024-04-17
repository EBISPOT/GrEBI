


use std::collections::{HashSet, HashMap};
use std::ops::Deref;
use std::{env, io};
use std::io::{BufRead, BufReader };
use std::io::{Write, BufWriter};

use grebi_shared::{get_subject, find_strings, serialize_equivalence, json_parser, json_lexer};
use grebi_shared::prefix_map::PrefixMap;
use grebi_shared::prefix_map::PrefixMapBuilder;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() {

    let stdin = io::stdin();
    let handle = stdin.lock();
    let mut reader = BufReader::new(handle);

    let stdout = io::stdout().lock();
    let mut writer = BufWriter::new(stdout);

    let normalise = {
        let rdr = BufReader::new( std::fs::File::open("prefix_map_normalise.json").unwrap() );
        let mut builder = PrefixMapBuilder::new();
        serde_json::from_reader::<_, HashMap<String, String>>(rdr).unwrap().into_iter().for_each(|(k, v)| {
            builder.add_mapping(k, v);
        });
        builder.build()
    };

    loop {
        let mut line: Vec<u8> = Vec::new();
        reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            break;
        }

        let string_locations_in_value = find_strings(&line);

        let mut n_ch = 0;
        let mut n_found_str = 0;

        while n_found_str < string_locations_in_value.len() {
            let (cur_start, cur_end) = string_locations_in_value[n_found_str];
            let str = &line[cur_start..cur_end];

            writer.write_all(&line[n_ch..cur_start]).unwrap();

            let reprefixed = normalise.reprefix_bytes(&str);

            if reprefixed.is_some() {
                writer.write_all(&reprefixed.unwrap()).unwrap();
            } else {
                writer.write_all(&str).unwrap();
            }

            n_ch = cur_end;
            n_found_str = n_found_str + 1;
        }

        writer.write_all(&line[n_ch..line.len()]).unwrap();
    }

    writer.flush().unwrap();

}



