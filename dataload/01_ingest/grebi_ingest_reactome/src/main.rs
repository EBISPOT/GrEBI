
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, self, BufReader, StdinLock, StdoutLock, Write, BufRead, Read};
use std::ptr::eq;
use std::env;
use clap::Parser;
use grebi_shared::prefix_map::PrefixMap;
use grebi_shared::prefix_map::PrefixMapBuilder;
use serde_json::json;
use serde_json::Value;

mod equivalences;
use crate::equivalences::equivalences;

fn main() {

    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);

    let stdout = io::stdout().lock();
    let mut output_nodes = BufWriter::new(stdout);




    let normalise:PrefixMap = {
        let rdr = BufReader::new( std::fs::File::open(env::var("GREBI_DATALOAD_HOME").unwrap().to_owned() + "/prefix_maps/prefix_map_normalise.json").unwrap() );
        let mut builder = PrefixMapBuilder::new();
        serde_json::from_reader::<_, HashMap<String, String>>(rdr).unwrap().into_iter().for_each(|(k, v)| {
            builder.add_mapping(k, v);
        });
        builder.build()
    };

    loop {
        let mut line_buf: Vec<u8> = Vec::new();
        reader.read_until(b'\n', &mut line_buf).unwrap();

        if line_buf.len() == 0 {
            break;
        }

        let json:Value = serde_json::from_slice::<Value>(&line_buf).unwrap();

        let obj = json.as_object().unwrap();

        let neo_id = obj.get("id").unwrap().as_str().unwrap();
        let obj_type = obj.get("type").unwrap().as_str().unwrap();
        let properties = obj.get("properties").unwrap().as_object().unwrap();

        if obj_type.eq("node") {

            let labels = obj.get("labels").unwrap().as_array().unwrap();
            let id = "reactome_".to_owned() + neo_id;

            let mut out_props = serde_json::Map::new();
            for (k, v) in properties {
                out_props.insert("reactome:".to_owned() + k, v.clone());
            }
            out_props.insert("grebi:type".to_string(), Value::Array(labels.iter().map(|v| Value::String("reactome:".to_owned() + v.as_str().unwrap())).collect::<Vec<Value>>()));

            let equivalences: Vec<Value> = equivalences(properties, &normalise)
                .into_iter().map(Value::String).collect();

            if equivalences.len() > 0 {
                out_props.insert("grebi:equivalentTo".to_string(), Value::Array( equivalences));
            }

            out_props.insert("id".to_string(), Value::String(id));

            output_nodes.write_all(serde_json::to_string(&out_props).unwrap().as_bytes()).unwrap();
            output_nodes.write_all("\n".as_bytes()).unwrap();

        } else if obj_type.eq("relationship") {

            let label = obj.get("label").unwrap().as_str().unwrap();
            let start_id = obj.get("start").unwrap().as_object().unwrap().get("id").unwrap().as_str().unwrap();
            let end_id = obj.get("end").unwrap().as_object().unwrap().get("id").unwrap().as_str().unwrap();

            let mut out_props = serde_json::Map::new();
            out_props.insert("id".to_string(), Value::String("reactome_".to_owned() + start_id));

            let new_props = serde_json::Map::new();
            for (k, v) in obj.get("properties").unwrap().as_object().unwrap() {
                out_props.insert("reactome:".to_owned() + k, v.clone());
            }

            out_props.insert("reactome:".to_owned() + label, json!([{
                "grebi:value": Value::String("reactome_".to_owned() + end_id),
                "grebi:properties": arrayify( Value::Object( new_props ) )
            }]));

            output_nodes.write_all(serde_json::to_string(&out_props).unwrap().as_bytes()).unwrap();
            output_nodes.write_all("\n".as_bytes()).unwrap();

        } else {
            panic!("Unknown type: {}", obj_type);
        }


    }

    output_nodes.flush().unwrap();

}

fn arrayify(pv:Value) -> Value {
    let obj =pv.as_object().unwrap();
    let mut new_obj = serde_json::Map::new();
    for (k, v) in obj {
        if v.is_array() {
            new_obj.insert(k.clone(), v.clone());
        } else {
            new_obj.insert(k.clone(), json!([v.clone()]));
        }
    }
    return Value::Object(new_obj);
}

