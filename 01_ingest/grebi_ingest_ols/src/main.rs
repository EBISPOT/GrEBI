
use std::collections::{HashMap, BTreeMap};
use std::fs::File;
use std::io::{BufWriter, self, BufReader, StdinLock, StdoutLock, Write};
use std::ptr::eq;
use clap::Parser;
use grebi_shared::prefix_map::PrefixMap;
use grebi_shared::prefix_map::PrefixMapBuilder;
use grebi_shared::serialize_equivalence;
use struson::reader::{JsonStreamReader, JsonReader};
use serde_json::Value;
use serde_json::Map;

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
    let reader = BufReader::new(stdin);

    let datasource_name = args.datasource_name.as_str();

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

    let mut json = JsonStreamReader::new(reader);

    json.begin_object().unwrap();
    let ontologies = json.next_name().unwrap();
    if ontologies != "ontologies" {
        panic!();
    }
    json.begin_array().unwrap();
    while json.has_next().unwrap() {
        read_ontology(&mut json, &mut output_nodes, &mut output_equivalences, &normalise, &datasource_name);
    }
    json.end_array().unwrap();
    json.end_object().unwrap();

}

fn read_ontology(json: &mut JsonStreamReader<BufReader<StdinLock<'_>>>, output_nodes: &mut BufWriter<File>, output_equivalences: &mut BufWriter<File>, normalise: &PrefixMap, datasource_name: &str) {

    json.begin_object().unwrap();

    let mut metadata:BTreeMap<String,Value> = BTreeMap::new();
    let mut key = String::new();

    loop {
        key = json.next_name().unwrap().to_string();

        if key.eq("classes") || key.eq("properties") || key.eq("individuals") {
            break;
        }

        metadata.insert(normalise.reprefix(&key.to_string()), read_value(json, &normalise));
    }

    let ontology_id = metadata.get("ontologyId").unwrap().as_str().unwrap().to_string();
    let ontology_iri = metadata.get("iri");
    let datasource = datasource_name.to_string() + "." + ontology_id.as_str();

    if ontology_iri.is_some() {
        let iri = ontology_iri.unwrap().as_str().unwrap().to_string();
        let id_to_iri_equivalence = serialize_equivalence(ontology_id.as_bytes(), iri.as_bytes());
        if id_to_iri_equivalence.is_some() {
            output_equivalences.write(id_to_iri_equivalence.unwrap().as_slice()).unwrap();
        }
    }

    output_nodes.write_all(r#"{"subject":""#.as_bytes()).unwrap();
    output_nodes.write_all(ontology_id.as_bytes()).unwrap();
    output_nodes.write_all(r#"","datasource":""#.as_bytes()).unwrap();
    output_nodes.write_all(datasource.as_bytes()).unwrap();
    output_nodes.write_all(r#"","properties":{"#.as_bytes()).unwrap();

    let mut is_first= true;
    for k in metadata.keys() {
        if is_first {
            is_first = false;
        } else {
            output_nodes.write_all(r#","#.as_bytes()).unwrap();
        }
        output_nodes.write_all(r#"""#.as_bytes()).unwrap();
        output_nodes.write_all(k.as_bytes()).unwrap();
        output_nodes.write_all(r#"":"#.as_bytes()).unwrap();
        output_nodes.write_all(metadata.get(k).unwrap().to_string().as_bytes()).unwrap();
        output_nodes.write_all(r#"""#.as_bytes()).unwrap();
    }

    output_nodes.write_all(r#"}}"#.as_bytes()).unwrap();
    output_nodes.write_all("\n".as_bytes()).unwrap();

    loop {
        if key.eq("classes") {
            read_entities(json, output_nodes, output_equivalences, normalise, &datasource);
        } else if key.eq("properties") {
            read_entities(json, output_nodes, output_equivalences, normalise, &datasource);
        } else if key.eq("individuals") {
            read_entities(json, output_nodes, output_equivalences, normalise, &datasource);
        } else {
            panic!();
        }
        if json.has_next().unwrap() {
            key = json.next_name().unwrap().to_string();
        } else {
            break;
        }
    }

    json.end_object().unwrap();

}

const EQUIV_PREDICATES :[&str;8]= [
    "owl:equivalentClass",
    "skos:exactMatch",
    "oboinowl:hasAlternativeId",
    "uniprot:replaces",
    "iao:0100001", // -> replacement term
    "obo:chebi/inchi",
    "obo:chebi/inchikey",
    "obo:chebi/smiles"
];

fn read_entities(json: &mut JsonStreamReader<BufReader<StdinLock<'_>>>, output_nodes: &mut BufWriter<File>, output_equivalences: &mut BufWriter<File>, normalise: &PrefixMap, datasource:&String) {
    json.begin_array().unwrap();
    while json.has_next().unwrap() {
        let mut val:Value = read_value(json, normalise);
        let mut obj = val.as_object_mut().unwrap();

        // should already be compacted
        let iri = obj.get("iri").unwrap().as_str().unwrap();

        output_nodes.write_all(r#"{"subject":""#.as_bytes()).unwrap();
        output_nodes.write_all(iri.as_bytes()).unwrap();
        output_nodes.write_all(r#"","datasource":""#.as_bytes()).unwrap();
        output_nodes.write_all(datasource.as_bytes()).unwrap();
        output_nodes.write_all(r#"","properties":{"#.as_bytes()).unwrap();

        let mut is_first = true;

        if is_first {
            is_first = false;
        } else {
            output_nodes.write_all(r#","#.as_bytes()).unwrap();
        }

        for k in obj.keys() {

            let v= obj.get(k).unwrap();

            if EQUIV_PREDICATES.contains(&k.as_str()) {
                if v.is_string() {
                    let equiv_subj = v.as_str().unwrap();
                    let equivalence = serialize_equivalence(iri.as_bytes(), equiv_subj.as_bytes());
                    if equivalence.is_some() {
                        output_equivalences.write(equivalence.unwrap().as_slice()).unwrap();
                    }
                } else if v.is_array() {
                    for equiv in v.as_array().unwrap() {
                        if equiv.is_string() {
                            let equiv_subj = equiv.as_str().unwrap();
                            let equivalence = serialize_equivalence(iri.as_bytes(), equiv_subj.as_bytes());
                            if equivalence.is_some() {
                                output_equivalences.write(equivalence.unwrap().as_slice()).unwrap();
                            }
                        }
                    }
                }
            }

            output_nodes.write_all(r#"""#.as_bytes()).unwrap();
            output_nodes.write_all(k.as_bytes()).unwrap();
            output_nodes.write_all(r#"":"#.as_bytes()).unwrap();
            output_nodes.write_all(v.to_string().as_bytes()).unwrap();
        }

        output_nodes.write_all(r#"}}"#.as_bytes()).unwrap();
        output_nodes.write_all("\n".as_bytes()).unwrap();
    }
    json.end_array().unwrap();
}

fn read_value(json: &mut JsonStreamReader<BufReader<StdinLock<'_>>>, normalise: &PrefixMap) -> Value {
    match json.peek().unwrap() {
        struson::reader::ValueType::Array => {
            let mut elems:Vec<Value> = Vec::new();
            json.begin_array().unwrap();
            while json.has_next().unwrap() {
                elems.push(read_value(json, normalise));
            }
            json.end_array().unwrap();
            return Value::Array(elems);
        }
        struson::reader::ValueType::Object => {
            let mut obj:Map<String,Value> = Map::new();
            json.begin_object().unwrap();
            while json.has_next().unwrap() {
                let k = json.next_name_owned().unwrap();
                if k.eq("type") {
                    read_value(json, &normalise); // skip
                    continue; // bnode/literal/iri; don't care for grebi
                }
                obj.insert( normalise.reprefix(&k), read_value(json, normalise));
            }
            json.end_object().unwrap();
            // if it was just a type and a value, the value is enough for grebi...
            if obj.len() == 1 && obj.contains_key("value") {
                return obj.get("value").unwrap().clone();
            }
            return Value::Object(obj);
        }
        struson::reader::ValueType::String => {
            return Value::String( normalise.reprefix( &json.next_string().unwrap().to_string() ));
        }
        struson::reader::ValueType::Number => {
            return Value::Number(json.next_number().unwrap().unwrap());
        }
        struson::reader::ValueType::Boolean => {
            return Value::Bool(json.next_bool().unwrap());
        }
        struson::reader::ValueType::Null => {
            json.next_null().unwrap();
            return Value::Null;
        }
    }
}



