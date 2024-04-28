
use std::collections::HashMap;
use std::io::{BufWriter, self, BufReader, Write,BufRead};
use clap::Parser;
use serde_json::{self, de, Map};
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

    #[arg(long)]
    json_de_nest_field:Option<Vec<String>>,
}

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
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

    // field -> nested field containing the actual value
    let mut de_nests:HashMap<String,String> = HashMap::new();
    if args.json_de_nest_field.is_some() {
        for arg in args.json_de_nest_field.unwrap() {
            let delim = arg.find('.').unwrap();
            let (field,subfield)=(arg[0..delim].to_string(), arg[delim+1..].to_string());
            de_nests.insert(field.clone(),subfield.clone());
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

            if v.is_null() {
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
            let denest_subfield:Option<&String> = de_nests.get(k);

            let new_v = map_value(v, inject_prefix, denest_subfield);

            out_json.insert(new_k, new_v);
        }

        output_nodes.write_all(Value::Object(out_json).to_string().as_bytes()).unwrap();
        output_nodes.write_all("\n".as_bytes()).unwrap();
    }

    output_nodes.flush().unwrap();
}

fn map_value(v:&Value, inject_prefix:Option<&String>, denest_subfield:Option<&String>) -> Value {
    if v.is_array() {
        return Value::Array(v.as_array().unwrap().iter().map(|v2| map_value(v2, inject_prefix, denest_subfield)).collect())
    }

    if denest_subfield.is_some() && v.is_object() {
        let subfield = denest_subfield.unwrap();
        let subfield_value = v.get(subfield);
        if subfield_value.is_some() {
            let mut props_obj:Map<String,Value> = Map::new();

            for (k,v) in v.as_object().unwrap().iter() {
                if k.eq(subfield) {
                    continue;
                }
                props_obj.insert(k.clone(), {
                    if v.is_array() {
                        v.clone()
                    } else {
                        Value::Array([v.clone()].to_vec())
                    }
                });
            }

            return json!({
                "grebi:value": subfield_value.unwrap(),
                "grebi:properties": props_obj
            });
        }
    }

    if inject_prefix.is_some() {
        return cloned_with_prefix(v, inject_prefix.unwrap())
    } else {
        return v.clone()
    }
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
