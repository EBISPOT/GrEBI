
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, self, BufReader, StdinLock, StdoutLock, Write};
use std::ptr::eq;
use clap::Parser;
use grebi_shared::prefix_map::PrefixMap;
use grebi_shared::prefix_map::PrefixMapBuilder;
use grebi_shared::serialize_equivalence;
use struson::reader::{JsonReader, JsonStreamReader, ValueType};
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
    output_equivalences:String,

    #[arg(long)]
    ontologies:String
}

fn main() {

    let args = Args::parse();

    eprintln!("args: {:?}", args);

    let stdin = io::stdin().lock();
    let reader = BufReader::new(stdin);

    let datasource_name = args.datasource_name.as_str();

    let mut output_nodes = BufWriter::new(
        File::create(args.output_nodes.as_str()).unwrap());

    let mut output_equivalences = BufWriter::new(
         File::create(args.output_equivalences.as_str()).unwrap());
    // output_equivalences.write_all(b"subject_id\tobject_id\n").unwrap();

    let mut ontology_whitelist:HashSet<String> = HashSet::new();
    for ontology in args.ontologies.split(",") {
        ontology_whitelist.insert(ontology.to_string());
    }


    let normalise = {
        let rdr = BufReader::new( std::fs::File::open("prefix_map_normalise.json").unwrap() );
        let mut builder = PrefixMapBuilder::new();
        serde_json::from_reader::<_, HashMap<String, String>>(rdr).unwrap().into_iter().for_each(|(k, v)| {
            builder.add_mapping(k, v);
        });
        builder.build()
    };


    // write normalise.buf to a file



    let mut json = JsonStreamReader::new(reader);

    json.begin_object().unwrap();
    let ontologies = json.next_name().unwrap();
    if ontologies != "ontologies" {
        panic!();
    }
    json.begin_array().unwrap();
    while json.has_next().unwrap() {
        read_ontology(&mut json, &mut output_nodes, &mut output_equivalences, &normalise, &datasource_name, &ontology_whitelist);
    }
    json.end_array().unwrap();
    json.end_object().unwrap();

}

fn read_ontology(json: &mut JsonStreamReader<BufReader<StdinLock<'_>>>, output_nodes: &mut BufWriter<File>, output_equivalences: &mut BufWriter<File>, normalise: &PrefixMap, datasource_name: &str, ontology_whitelist:&HashSet<String>) {

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

    if !ontology_whitelist.contains(&ontology_id) {
        json.skip_value().unwrap();
        while json.has_next().unwrap() {
            json.skip_name().unwrap();
            json.skip_value().unwrap();
        }
        json.end_object().unwrap();
        return;
    }


    let ontology_iri = metadata.get("iri");
    let datasource = datasource_name.to_string() + "." + ontology_id.as_str();

    if ontology_iri.is_some() {
        let iri = ontology_iri.unwrap().as_str().unwrap().to_string();
        let id_to_iri_equivalence = serialize_equivalence(ontology_id.as_bytes(), normalise.reprefix(&iri.to_string()).as_bytes());
        if id_to_iri_equivalence.is_some() {
            output_equivalences.write(id_to_iri_equivalence.unwrap().as_slice()).unwrap();
        }
    }

    output_nodes.write_all(r#"{"subject":""#.as_bytes()).unwrap();
    output_nodes.write_all(ontology_id.as_bytes()).unwrap();
    output_nodes.write_all(r#"","datasource":""#.as_bytes()).unwrap();
    output_nodes.write_all(datasource.as_bytes()).unwrap();
    output_nodes.write_all(r#"","properties":{"#.as_bytes()).unwrap();

    let mut is_first = true;

    for k in metadata.keys() {

        if is_first {
            is_first = false;
        } else {
            output_nodes.write_all(r#","#.as_bytes()).unwrap();
        }

        let v= metadata.get(k).unwrap();

        output_nodes.write_all(r#"""#.as_bytes()).unwrap();
        output_nodes.write_all(k.as_bytes()).unwrap();
        output_nodes.write_all(r#"":"#.as_bytes()).unwrap();

        if v.is_array() {
            output_nodes.write_all(v.to_string().as_bytes()).unwrap();
        } else {
            output_nodes.write_all(r#"["#.as_bytes()).unwrap();
            output_nodes.write_all(v.to_string().as_bytes()).unwrap();
            output_nodes.write_all(r#"]"#.as_bytes()).unwrap();
        }
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

const EQUIV_PREDICATES :[&str;2]= [
    "owl:equivalentClass",
    "owl:equivalentProperty",
    // "owl:sameAs",
    // "skos:exactMatch",
    // "oboinowl:hasAlternativeId",
    // "uniprot:replaces",
    // "iao:0100001" // -> replacement term
];

fn read_entities(json: &mut JsonStreamReader<BufReader<StdinLock<'_>>>, output_nodes: &mut BufWriter<File>, output_equivalences: &mut BufWriter<File>, normalise: &PrefixMap, datasource:&String) {
    json.begin_array().unwrap();
    while json.has_next().unwrap() {
        let mut val:Value = read_value(json, normalise);
        let obj = val.as_object_mut().unwrap();

        let curie = normalise.reprefix(& obj.get("iri").unwrap().as_str().unwrap().to_string() );

        for k in obj.keys() {
            if k.eq("iri") {
                continue
            }
            let v = obj.get(k).unwrap();
            if EQUIV_PREDICATES.contains(&k.as_str()) {
                for equiv in get_string_values(v) {
                    let equivalence = serialize_equivalence(curie.as_bytes(), normalise.reprefix(&equiv.to_string()).as_bytes());
                    if equivalence.is_some() {
                        output_equivalences.write(equivalence.unwrap().as_slice()).unwrap();
                    }
                }
            } else if k.eq("obo:chebi/inchi") {
                for equiv in get_string_values(v) {
                    let equivalence = serialize_equivalence(curie.as_bytes(), ("inchi:".to_owned()+&equiv.to_string()).as_bytes());
                    if equivalence.is_some() {
                        output_equivalences.write(equivalence.unwrap().as_slice()).unwrap();
                    }
                }
            } else if k.eq("obo:chebi/inchikey") {
                for equiv in get_string_values(v) {
                    let equivalence = serialize_equivalence(curie.as_bytes(), ("inchikey:".to_owned()+&equiv.to_string()).as_bytes());
                    if equivalence.is_some() {
                        output_equivalences.write(equivalence.unwrap().as_slice()).unwrap();
                    }
                }
            } else if k.eq("obo:chebi/smiles") {
                for equiv in get_string_values(v) {
                    let equivalence = serialize_equivalence(curie.as_bytes(), ("smiles:".to_owned()+&equiv.to_string()).as_bytes());
                    if equivalence.is_some() {
                        output_equivalences.write(equivalence.unwrap().as_slice()).unwrap();
                    }
                }
            }
        }

        output_nodes.write_all(r#"{"subject":""#.as_bytes()).unwrap();
        output_nodes.write_all(curie.as_bytes()).unwrap();
        output_nodes.write_all(r#"","datasource":""#.as_bytes()).unwrap();
        output_nodes.write_all(datasource.as_bytes()).unwrap();
        output_nodes.write_all(r#"","properties":{"#.as_bytes()).unwrap();

        let mut is_first = true;

        for k in obj.keys() {

            if is_first {
                is_first = false;
            } else {
                output_nodes.write_all(r#","#.as_bytes()).unwrap();
            }

            if k.eq("searchableAnnotationValues") {
                continue;
            }

            let v= obj.get(k).unwrap();

            output_nodes.write_all(r#"""#.as_bytes()).unwrap();
            output_nodes.write_all(k.as_bytes()).unwrap();
            output_nodes.write_all(r#"":"#.as_bytes()).unwrap();

            output_nodes.write_all(r#"["#.as_bytes()).unwrap();
                if v.is_array() {
                    let mut arr_is_first = true;
                    for el in v.as_array().unwrap() {
                        if arr_is_first {
                            arr_is_first = false;
                        } else {
                            output_nodes.write_all(r#","#.as_bytes()).unwrap();
                        }
                        write_value(el, output_nodes);
                    }
                } else {
                    write_value(&v, output_nodes);
                }
            output_nodes.write_all(r#"]"#.as_bytes()).unwrap();

        }
        output_nodes.write_all(r#"}}"#.as_bytes()).unwrap();
        output_nodes.write_all("\n".as_bytes()).unwrap();
    }
    json.end_array().unwrap();
}

fn write_value(v:&Value, output_nodes: &mut BufWriter<File>) {
    if v.is_array() {
        output_nodes.write_all(r#"["#.as_bytes()).unwrap();
            let mut is_first = true;
            for el in v.as_array().unwrap() {
                if is_first {
                    is_first = false;
                } else {
                    output_nodes.write_all(r#","#.as_bytes()).unwrap();
                }
                write_value(el, output_nodes);
            }
        output_nodes.write_all(r#"]"#.as_bytes()).unwrap();
        return;
    }

    if v.is_object() {
        let obj = v.as_object().unwrap();
        let obj_types = obj.get("type");

        if obj_types.is_some() {
            if has_reification_type(obj_types.unwrap().as_array().unwrap()) {
                let reified_value = obj.get("value").unwrap();
                let axiom_sets = obj.get("axioms").unwrap().as_array().unwrap();
                for axiom_set in axiom_sets {
                    let reified_props = axiom_set.as_object().unwrap();

                    output_nodes.write_all(r#"{"value":"#.as_bytes()).unwrap();
                    write_value(reified_value, output_nodes);
                    output_nodes.write_all(r#","properties":{"#.as_bytes()).unwrap();
                        let mut is_first = true;
                        for k in reified_props.keys() {
                            if is_first {
                                is_first = false;
                            } else {
                                output_nodes.write_all(r#","#.as_bytes()).unwrap();
                            }
                            let v= reified_props.get(k).unwrap();
                            output_nodes.write_all(r#"""#.as_bytes()).unwrap();
                            output_nodes.write_all(k.as_bytes()).unwrap();
                            output_nodes.write_all(r#"":"#.as_bytes()).unwrap();
                            output_nodes.write_all(r#"["#.as_bytes()).unwrap();
                                if v.is_array() {
                                    let mut arr_is_first = true;
                                    for el in v.as_array().unwrap() {
                                        if arr_is_first {
                                            arr_is_first = false;
                                        } else {
                                            output_nodes.write_all(r#","#.as_bytes()).unwrap();
                                        }
                                        write_value(el, output_nodes);
                                    }
                                } else {
                                    write_value(&v, output_nodes);
                                }
                            output_nodes.write_all(r#"]"#.as_bytes()).unwrap();
                        }
                    output_nodes.write_all(r#"}}"#.as_bytes()).unwrap();
                }
            } else {
                let value = obj.get("value").unwrap();
                write_value(&value, output_nodes);
            }
            return;
        } else {
            output_nodes.write_all(r#"{"#.as_bytes()).unwrap();
            let mut is_first = true;
            for (k,v) in obj {
                if is_first {
                    is_first = false;
                } else {
                    output_nodes.write_all(r#","#.as_bytes()).unwrap();
                }
                output_nodes.write_all(r#"""#.as_bytes()).unwrap();
                output_nodes.write_all(k.as_bytes()).unwrap();
                output_nodes.write_all(r#"":"#.as_bytes()).unwrap();
                write_value(v, output_nodes);
            }
            output_nodes.write_all(r#"}"#.as_bytes()).unwrap();
            return;
        }
    }

    output_nodes.write_all(v.to_string().as_bytes()).unwrap();
}

fn has_reification_type(v:&Vec<Value>) -> bool {
    for el in v {
        if el.as_str().unwrap() == "reification" {
            return true;
        }
    }
    return false;
}

fn get_string_values<'a>(v:&'a Value) ->Vec<&'a str> {
    if v.is_string() {
        return [v.as_str().unwrap()].to_vec();
    }
    if v.is_object() {
        let value = v.get("value");
        if value.is_some() {
            return get_string_values(value.unwrap());
        }
    }
    if v.is_array() {
        return v.as_array().unwrap().iter().map(|x| get_string_values(x)).flatten().collect();
    }
    return [].to_vec();
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
                obj.insert( normalise.reprefix(&k), read_value(json, normalise));
            }
            json.end_object().unwrap();
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






