
use std::collections::HashMap;
use std::io::{BufWriter, self, BufReader, Write,BufRead};
use clap::Parser;
use serde_json;
use serde_json::Value;
use serde_json::json;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    datasource_name: String,

    #[arg(long)]
    filename: String,

    #[arg(long)]
    json_rename_field:Option<Vec<String>>,

    #[arg(long, default_value_t = String::from(""))]
    json_inject_type:String,

    #[arg(long, default_value_t = String::from(""))]
    json_inject_key_prefix:String,

    #[arg(long)]
    json_inject_value_prefix:Option<Vec<String>>,
}

fn main() {

    let args = Args::parse();

    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);

    let stdout = io::stdout().lock();
    let mut output_nodes = BufWriter::new(stdout);

    let mut renames:HashMap<String,String> = HashMap::new();

    if args.json_rename_field.is_some() {
        for arg in args.json_rename_field.unwrap() {
            let delim = arg.find(':').unwrap();
            let (column,rename)=(arg[0..delim].to_string(), arg[delim+1..].to_string());
            renames.insert(column.clone(), rename.clone());
        }
    }

    let mut value_prefixes:HashMap<String,String> = HashMap::new();

    if args.json_inject_value_prefix.is_some() {
        for arg in args.json_inject_value_prefix.unwrap() {
            let delim = arg.find(':').unwrap();
            let (column,prefix)=(arg[0..delim].to_string(), arg[delim+1..].to_string());
            value_prefixes.insert(column.clone(),prefix.clone());
        }
    }

    loop {

        let mut line:Vec<u8> = Vec::new();
        reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            break;
        }

        let json:serde_json::Map<String,Value> = serde_json::from_slice(&line).unwrap();
        let mut out_json = serde_json::Map::new();

        if args.json_inject_type.len() > 0 {
            out_json.insert("grebi:type".to_string(), json!([args.json_inject_type]));
        }

        for (k,v) in json.iter() {

            if k.eq("id") {
                out_json.insert(k.clone(), v.clone());
                continue;
            }

            let new_k = {
                let alias = renames.get(k);
                if alias.is_some() {
                    alias.unwrap().clone()
                } else {
                    if args.json_inject_key_prefix.len() > 0 && k.find(":").is_none() {
                        args.json_inject_key_prefix.clone() + k
                    } else {
                        k.clone()
                    }
                }
            };

            let inject_prefix = value_prefixes.get(k);

            let new_v = {
                if v.is_array() {
                    Value::Array(v.as_array().unwrap().iter().map(|v2| {
                        if inject_prefix.is_some() {
                            cloned_with_prefix(v2, inject_prefix.unwrap())
                        } else {
                            v2.clone()
                        }
                    }).collect())
                } else {
                    if inject_prefix.is_some() {
                        cloned_with_prefix(v, inject_prefix.unwrap())
                    } else {
                        v.clone()
                    }
                }
            };

            out_json.insert(new_k, new_v);
        }

        output_nodes.write_all(Value::Object(out_json).to_string().as_bytes()).unwrap();
        output_nodes.write_all("\n".as_bytes()).unwrap();
    }

    output_nodes.flush().unwrap();
}

fn cloned_with_prefix(val:&Value, prefix:&str) -> Value {
    if val.is_string() {
        json!(prefix.to_owned() + val.as_str().unwrap())
    } else if val.is_f64() {
        json!(prefix.to_owned() + &val.as_f64().unwrap().to_string())
    } else if val.is_i64() {
        json!(prefix.to_owned() + &val.as_i64().unwrap().to_string())
    } else {
        val.clone()
    }
}
