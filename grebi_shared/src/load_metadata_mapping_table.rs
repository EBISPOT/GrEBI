use std::{collections::BTreeMap, fs::File, io::{BufRead, BufReader}};

use crate::{json_lexer::{JsonToken, JsonTokenType}, json_parser, slice_merged_entity};


pub struct Metadata {
    pub json:Vec<u8>,
}

pub fn load_metadata_mapping_table<'a>(metadata_jsonl_path:&str) -> BTreeMap<Vec<u8>, Metadata> {

    let start_time = std::time::Instant::now();

    let mut reader = BufReader::new(File::open(&metadata_jsonl_path).unwrap());
    let mut res:BTreeMap<Vec<u8>, Metadata> = BTreeMap::new();

    loop {
        let mut line:Vec<u8> = Vec::new();
        reader.read_until(b'\n', &mut line).unwrap();
        if line.len() == 0 {
            break;
        }
        if line[line.len() - 1] == b'\n' {
            line.pop();
        }

        let mut id:Option<Vec<u8>> = None;

        {
            let mut parser = json_parser::JsonParser::parse(&line);

            parser.begin_object();
            while parser.peek().kind != JsonTokenType::EndObject {
                let name = parser.name();
                if name == b"grebi:nodeId" {
                    id = Some(parser.string().to_vec());
                } else {
                    parser.value(); // skip
                }
            }
        }

        res.insert(id.unwrap().clone(), Metadata { json:line });
    }

    eprintln!("loaded {} metadata objects in {} seconds", res.len(), start_time.elapsed().as_secs());

    return res;
}
