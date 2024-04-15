
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, self, BufReader, StdinLock, StdoutLock, Write, BufRead, Read};
use std::ptr::eq;
use clap::Parser;
use grebi_shared::prefix_map::PrefixMap;
use grebi_shared::prefix_map::PrefixMapBuilder;
use grebi_shared::serialize_equivalence;
use serde_json::json;
use serde_json::Value;

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
    output_equivalences:String
}

fn main() {

    let args = Args::parse();

    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);

    let datasource_name = args.datasource_name.as_str();

    let mut output_nodes = BufWriter::new(
        File::create(args.output_nodes.as_str()).unwrap());

    let mut output_equivalences = BufWriter::new(
         File::create(args.output_equivalences.as_str()).unwrap());

    let normalise:PrefixMap = {
        let rdr = BufReader::new( std::fs::File::open("prefix_map_normalise.json").unwrap() );
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

            let mut out_props = properties.clone();
            out_props.insert("grebi:type".to_string(), Value::Array(labels.iter().map(|v| Value::String("reactome:".to_owned() + v.as_str().unwrap())).collect::<Vec<Value>>()));

            output_nodes.write_all(serde_json::to_string(&normalise_ids(json!({
                "subject": id,
                "datasource": datasource_name,
                "properties": arrayify( Value::Object(out_props) ) 
            }), &normalise)).unwrap().as_bytes()).unwrap();
            output_nodes.write_all("\n".as_bytes()).unwrap();

            // Equivalences
            // 
            let p_url = properties.get("url");
            if p_url.is_some() {
                let url = p_url.unwrap().as_str().unwrap();

                // if we can compact the url with bioregistry then we have a curie which we can use as the ID
                let reprefixed = normalise.maybe_reprefix(&url.to_string());
                if reprefixed.is_some() {
                    output_equivalences.write_all(&serialize_equivalence(id.as_bytes(), reprefixed.unwrap().as_bytes()).unwrap()).unwrap();
                }
            }

            // see if the identifier works with bioregistry
            let p_id = properties.get("identifier");
            if p_id.is_some() {
                let reprefixed = normalise.maybe_reprefix(&p_id.unwrap().as_str().unwrap().to_owned());
                if reprefixed.is_some() {
                    output_equivalences.write_all(&serialize_equivalence(id.as_bytes(), reprefixed.unwrap().as_bytes()).unwrap()).unwrap();
                }
            } else {
                // try mashing the databaseName and identifier together as a curie and see if it works with bioregistry
                let p_dbname = properties.get("databaseName");
                if p_dbname.is_some() && p_id.is_some() {
                    let curie = p_dbname.unwrap().as_str().unwrap().to_owned() + ":" + p_id.unwrap().as_str().unwrap();
                    let reprefixed = normalise.maybe_reprefix(&curie);
                    if reprefixed.is_some() {
                        output_equivalences.write_all(&serialize_equivalence(id.as_bytes(), reprefixed.unwrap().as_bytes()).unwrap()).unwrap();
                    }
                }
            }

        } else if obj_type.eq("relationship") {

            let label = obj.get("label").unwrap().as_str().unwrap();
            let start_id = obj.get("start").unwrap().as_object().unwrap().get("id").unwrap().as_str().unwrap();
            let end_id = obj.get("end").unwrap().as_object().unwrap().get("id").unwrap().as_str().unwrap();

            let mut out_props = serde_json::Map::new();
            out_props.insert("reactome:".to_owned() + label, json!([{
                "value": Value::String("reactome_".to_owned() + end_id),
                "properties": arrayify( obj.get("properties").unwrap().clone() )
            }]));

            output_nodes.write_all(serde_json::to_string(&normalise_ids(json!({
                "subject": "reactome_".to_owned() + start_id,
                "datasource": datasource_name,
                "properties": out_props
            }), &normalise)).unwrap().as_bytes()).unwrap();

            output_nodes.write_all("\n".as_bytes()).unwrap();

        } else {
            panic!("Unknown type: {}", obj_type);
        }


    }

    output_nodes.flush().unwrap();
    output_equivalences.flush().unwrap();

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

fn normalise_ids(json:Value, normalise:&PrefixMap)->Value {
    if json.is_array() {
        return Value::Array(json.as_array().unwrap().iter().map(|v| normalise_ids(v.clone(), &normalise)).collect::<Vec<Value>>());
    }
    if json.is_object() {
        let obj = json.as_object().unwrap();
        let mut new_obj = serde_json::Map::new();
        for (k, v) in obj {
            new_obj.insert( normalise.reprefix(k), normalise_ids(v.clone(), &normalise));
        }
        return Value::Object(new_obj);
    }
    if json.is_string() {
        return Value::String(normalise.reprefix(&json.as_str().unwrap().to_string()));
    }
    return json.clone();
}
