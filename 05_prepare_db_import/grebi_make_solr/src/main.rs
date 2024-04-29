

use std::ascii::escape_default;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::BufWriter;
use std::io::BufReader;
use std::io::Write;
use std::io;
use std::io::BufRead;
use std::mem::transmute;

use clap::Parser;
use grebi_shared::json_lexer::JsonTokenType;
use grebi_shared::json_parser;
use grebi_shared::prefix_map::PrefixMap;

use serde_json::Value;
use grebi_shared::slice_merged_entity::SlicedEntity;
use grebi_shared::slice_merged_entity::SlicedProperty;
use grebi_shared::slice_merged_entity::SlicedReified;

use grebi_shared::json_lexer::{lex, JsonToken };
use grebi_shared::json_parser::JsonParser;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    in_names_txt: String,

    #[arg(long)]
    in_metadata_json_path: String,

    #[arg(long)]
    out_csv_path: String,
}

fn main() -> std::io::Result<()> {

    let args = Args::parse();

    let start_time = std::time::Instant::now();

    let subj_to_name:BTreeMap<String,String> = {
        let start_time = std::time::Instant::now();
        let mut res:BTreeMap<String,String> = BTreeMap::new();
        let mut reader = BufReader::new(File::open(&args.in_names_txt).unwrap());
        loop {
            let mut subject: Vec<u8> = Vec::new();
            reader.read_until(b'\t', &mut subject).unwrap();
            if subject.len() == 0 {
                break;
            }
            subject.pop();
            let mut name: Vec<u8> = Vec::new();
            reader.read_until(b'\n', &mut name).unwrap();
            if name.len() == 0 {
                continue;
            }
            if name[name.len() - 1] == b'\n' {
                name.pop();
            }
            res.insert(String::from_utf8(subject).unwrap(), String::from_utf8(name).unwrap());
        }
        eprintln!("loaded {} subject->name mappings in {} seconds", res.len(), start_time.elapsed().as_secs());
        res
    };

    let index_metadata:Value = serde_json::from_reader(File::open(args.in_metadata_json_path).unwrap()).unwrap();

    let all_entity_props: Vec<String> = index_metadata["entity_props"].as_object().unwrap().keys().cloned().collect();
    let all_edge_props: Vec<String> = index_metadata["edge_props"].as_object().unwrap().keys().cloned().collect();

    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);

    let mut writer =
        BufWriter::with_capacity(1024*1024*32,
            // GzEncoder::new(
            File::create(args.out_csv_path).unwrap()
            // Compression::fast())
        );

    let mut n_nodes:i64 = 0;

    loop {
        let mut line: Vec<u8> = Vec::new();
        reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            break;
        }

        let json:serde_json::Map<String,Value> = serde_json::from_slice(&line).unwrap();
        let mut out_json = serde_json::Map::new();

        for (k,v) in json.iter() {

            if k.eq("grebi:nodeId") || k.eq("grebi:datasources") {
                out_json.insert(k.to_string(), v.clone());
                continue;
            }

            let arr = v.as_array().unwrap();
            let mut new_arr:Vec<Value> = Vec::new();

            for i in 0..arr.len() {
                new_arr.extend(value_to_solr(&arr[i], &subj_to_name));
            }

            if new_arr.len() > 0 {
                out_json.insert(k.clone(), Value::Array(new_arr));
            }
        }

        writer.write_all(serde_json::to_string(&out_json).unwrap().as_bytes()).unwrap();
        writer.write_all(b"\n").unwrap();
    }

    writer.flush().unwrap();

    eprintln!("prepare_db_import for solr took {} seconds", start_time.elapsed().as_secs());

    Ok(())
}

fn value_to_solr(v:&Value, subj_to_name:&BTreeMap<String,String>) -> Vec<Value> {

    let obj = v.as_object().unwrap();

    // ... ignore datasources ...

    let value = obj.get("grebi:value").unwrap();



    // nested arrays - not stored in solr currently
    //
    if value.is_array() {
        return vec!();
    }

    // string values -  relatively simple case
    // an ID expands to both the id and the name of what the id points to
    // any other string is left unmodified
    //
    if value.is_string() {
        let maps_to_name = subj_to_name.get(value.as_str().unwrap());
        if maps_to_name.is_some() {
            // add both the ID and its label
            return vec!(value.clone(), Value::String(maps_to_name.unwrap().to_string()));
        } else {
            return vec!(value.clone());
        }
    }

    // for objects, if its a reification ignore it
    // otherwise not stored in solr
    //
    if value.is_object() {
        let vobj = value.as_object().unwrap();
        if vobj.contains_key("grebi:value") {
            let the_actual_value = vobj.get("grebi:value").unwrap();    
            if the_actual_value.is_string() {
                let maps_to_name = subj_to_name.get(the_actual_value.as_str().unwrap());
                if maps_to_name.is_some() {
                    // add both the ID and its label
                    return vec!(value.clone(), Value::String(maps_to_name.unwrap().to_string()));
                } else {
                    return vec!(the_actual_value.clone());
                }
            }
        }
    }

    return vec!();

}


