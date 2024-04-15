
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, self, BufReader, StdinLock, StdoutLock, Write,BufRead};
use std::ptr::eq;
use clap::Parser;
use grebi_shared::prefix_map::PrefixMap;
use grebi_shared::prefix_map::PrefixMapBuilder;
use grebi_shared::serialize_equivalence;
use grebi_shared::json_lexer;
use grebi_shared::json_parser;
use grebi_shared::json_lexer::JsonToken;
use grebi_shared::json_lexer::JsonTokenType;
use serde_json::json;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    datasource_name: String,

    #[arg(long)]
    filename: String,

    #[arg(long)]
    output_nodes:String,

    #[arg(long)]
    output_equivalences:String,

    #[arg(long)]
    json_subject_field:String,

    #[arg(long, default_value_t = String::from(""))]
    json_inject_type:String,

    #[arg(long, default_value_t = String::from(""))]
    json_inject_key_prefix:String
}

fn main() {

    let args = Args::parse();

    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);


    let mut output_nodes = BufWriter::new(
        File::create(args.output_nodes.as_str()).unwrap());

    let mut output_equivalences = BufWriter::new(
         File::create(args.output_equivalences.as_str()).unwrap());
    // output_equivalences.write_all(b"subject_id\tobject_id\n").unwrap();

    let normalise = {
        let rdr = BufReader::new( std::fs::File::open("prefix_map_normalise.json").unwrap() );
        let mut builder = PrefixMapBuilder::new();
        serde_json::from_reader::<_, HashMap<String, String>>(rdr).unwrap().into_iter().for_each(|(k, v)| {
            builder.add_mapping(k, v);
        });
        builder.build()
    };

    let subj_field:&[u8] = args.json_subject_field.as_bytes();

    let middle_json_fragment
         = [r#","datasource":""#.as_bytes(), args.datasource_name.as_bytes(), r#"""#.as_bytes() ].concat();


    loop {

        let mut line:Vec<u8> = Vec::new();
        reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            break;
        }

        let mut json = json_parser::JsonParser::from_lexed(json_lexer::lex(&line));

        output_nodes.write_all(r#"{"subject":"#.as_bytes()).unwrap();
        json.mark();
        json.begin_object();
        'write_subject: {
            while json.peek().kind != JsonTokenType::EndObject {
                let k = json.name(&line);
                if k == subj_field {
                    write_from_parser(&mut json, &line, &mut output_nodes, &normalise);
                    break 'write_subject;
                } else {
                    json.value(&line); // skip
                }
            }
            panic!("Subject field {} not found", args.json_subject_field);
        }
        json.rewind();

        output_nodes.write_all(&middle_json_fragment).unwrap();

        output_nodes.write_all(&",\"properties\":{".as_bytes()).unwrap();

        json.begin_object();
        let mut is_first = true;
        while json.peek().kind != JsonTokenType::EndObject {
            let k = json.name(&line);

            if k == subj_field {
                json.value(&line); // skip
                continue;
            } else {
                if is_first {
                    if args.json_inject_type.len() > 0 {
                        output_nodes.write_all(r#""grebi:type":[""#.as_bytes()).unwrap();
                        output_nodes.write_all(args.json_inject_type.as_bytes()).unwrap();
                        output_nodes.write_all(r#""],"#.as_bytes()).unwrap();
                    }
                    is_first = false;
                } else {
                    output_nodes.write_all(r#","#.as_bytes()).unwrap();
                }

                output_nodes.write_all(r#"""#.as_bytes()).unwrap();
                if args.json_inject_key_prefix.len() > 0 {
                    output_nodes.write_all(args.json_inject_key_prefix.as_bytes()).unwrap();
                }
                output_nodes.write_all(k).unwrap();
                output_nodes.write_all(r#"":"#.as_bytes()).unwrap();

                let tok = json.peek();
                // all prop values must be arrays
                if tok.kind == JsonTokenType::StartArray {
                    write_from_parser(&mut json, &line, &mut output_nodes, &normalise)
                } else {
                    output_nodes.write_all(r#"["#.as_bytes()).unwrap();
                    write_from_parser(&mut json, &line, &mut output_nodes, &normalise);
                    output_nodes.write_all(r#"]"#.as_bytes()).unwrap();
                }
            }
        }
        json.end_object();

        output_nodes.write_all(b"}}\n").unwrap();
    }

    output_nodes.flush().unwrap();
    output_equivalences.flush().unwrap();
}

fn write_from_parser(json:&mut json_parser::JsonParser, line:&Vec<u8>, output:&mut BufWriter<File>, normalise:&PrefixMap) {

    match json.peek().kind {
        JsonTokenType::StartObject => {
            output.write_all(r#"{"#.as_bytes()).unwrap();
            json.begin_object();
            let mut is_first = true;
            while json.peek().kind != JsonTokenType::EndObject {
                if is_first {
                    is_first = false;
                } else {
                    output.write_all(b",").unwrap();
                }
                let k = json.name(&line);
                output.write_all(r#"""#.as_bytes()).unwrap();
                output.write_all(k).unwrap();
                output.write_all(r#"":"#.as_bytes()).unwrap();
                write_from_parser(json, line, output, normalise);
            }
            json.end_object();
            output.write_all(r#"}"#.as_bytes()).unwrap();
        }
        JsonTokenType::StartArray => {
            output.write_all(r#"["#.as_bytes()).unwrap();
            let mut is_first = true;
            json.begin_array();
            while json.peek().kind != JsonTokenType::EndArray {
                if is_first {
                    is_first = false;
                } else {
                    output.write_all(b",").unwrap();
                }
                write_from_parser(json, line, output, normalise);
            }
            json.end_array();
            output.write_all(r#"]"#.as_bytes()).unwrap();
        }
        JsonTokenType::StartString => {
            let str = json.string(&line);
            let reprefixed = normalise.reprefix_bytes(str);
            output.write_all(r#"""#.as_bytes()).unwrap();
            if reprefixed.is_some() {
                output.write_all(&reprefixed.unwrap()).unwrap();
            } else {
                output.write_all(&str).unwrap();
            }
            output.write_all(r#"""#.as_bytes()).unwrap();
        }
        _ => {
            let v = json.value(&line);
            output.write_all(&v).unwrap();
        }
    }
}






