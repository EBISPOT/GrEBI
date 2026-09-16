use std::{collections::BTreeMap, fs::File, io::{BufRead, BufReader}};

use crate::{json_lexer::JsonTokenType, json_parser};


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


#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn indexes_each_line_by_its_node_id_wherever_the_id_sits() {
        let path = std::env::temp_dir().join(format!("grebi_shared_metadata_{}.jsonl", std::process::id()));
        File::create(&path).unwrap().write_all(
            b"{\"grebi:nodeId\":\"a\",\"x\":[1,2]}\n{\"y\":{\"z\":null},\"grebi:nodeId\":\"b\"}\n").unwrap();
        let table = load_metadata_mapping_table(path.to_str().unwrap());
        assert_eq!(table.len(), 2);
        assert_eq!(table.get(b"a" as &[u8]).unwrap().json, b"{\"grebi:nodeId\":\"a\",\"x\":[1,2]}");
        assert_eq!(table.get(b"b" as &[u8]).unwrap().json, b"{\"y\":{\"z\":null},\"grebi:nodeId\":\"b\"}", "the line is kept as is, without its newline");
        assert_eq!(table.keys().next().unwrap(), &b"a".to_vec(), "ordered by id");
    }
}
