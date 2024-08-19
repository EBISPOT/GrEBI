
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, self, BufReader, StdinLock, StdoutLock, Write};
use std::ptr::eq;
use clap::Parser;
use grebi_shared::prefix_map::PrefixMap;
use grebi_shared::prefix_map::PrefixMapBuilder;
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
    ontologies:String,

    #[arg(long)]
    defining_only:bool,

    #[arg(long)]
    skip_obsolete:bool

}

fn main() {

    let args = Args::parse();

    eprintln!("args: {:?}", args);

    let stdin = io::stdin().lock();
    let reader = BufReader::new(stdin);

    let datasource_name = args.datasource_name.as_str();

    let stdout = io::stdout().lock();
    let mut output_nodes = BufWriter::new(stdout);

    let mut ontology_whitelist:HashSet<String> = HashSet::new();
    for ontology in args.ontologies.split(",") {
        ontology_whitelist.insert(ontology.to_string());
    }


    let mut json = JsonStreamReader::new(reader);

    json.begin_object().unwrap();
    let ontologies = json.next_name().unwrap();
    if ontologies != "ontologies" {
        panic!();
    }
    json.begin_array().unwrap();
    while json.has_next().unwrap() {
        read_ontology(&mut json, &mut output_nodes, &datasource_name, &ontology_whitelist, args.defining_only, args.skip_obsolete);
    }
    json.end_array().unwrap();
    json.end_object().unwrap();

}

fn read_ontology(json: &mut JsonStreamReader<BufReader<StdinLock<'_>>>, output_nodes: &mut BufWriter<StdoutLock>, datasource_name: &str, ontology_whitelist:&HashSet<String>, defining_only:bool, skip_obsolete:bool) {

    json.begin_object().unwrap();

    let mut metadata:BTreeMap<String,Value> = BTreeMap::new();
    let mut key = String::new();

    loop {
        key = json.next_name().unwrap().to_string();

        if key.eq("classes") || key.eq("properties") || key.eq("individuals") {
            break;
        }

        metadata.insert(key, read_value(json));
    }

    let ontology_id = metadata.get("ontologyId").unwrap().as_str().unwrap().to_string();

    if !ontology_whitelist.contains(&ontology_id) {
        eprintln!("Skipping ontology: {}", ontology_id);
        json.skip_value().unwrap();
        while json.has_next().unwrap() {
            json.skip_name().unwrap();
            json.skip_value().unwrap();
        }
        json.end_object().unwrap();
        return;
    }

    eprintln!("Reading ontology: {}", ontology_id);

    let ontology_iri = metadata.get("iri");
    let datasource = datasource_name.to_string() + "." + ontology_id.as_str();

    output_nodes.write_all(r#"{"id":""#.as_bytes()).unwrap();
    output_nodes.write_all(ontology_id.as_bytes()).unwrap();
    output_nodes.write_all(r#"","grebi:datasource":""#.as_bytes()).unwrap();
    output_nodes.write_all(datasource.as_bytes()).unwrap();
    output_nodes.write_all(r#"","grebi:type":["ols:Ontology"]"#.as_bytes()).unwrap();

    for k in metadata.keys() {

        output_nodes.write_all(r#","#.as_bytes()).unwrap();

        let v= metadata.get(k).unwrap();

        output_nodes.write_all(r#"""#.as_bytes()).unwrap();
        output_nodes.write_all(reprefix_predicate(k).as_bytes()).unwrap();
        output_nodes.write_all(r#"":"#.as_bytes()).unwrap();

        if v.is_array() {
            output_nodes.write_all(v.to_string().as_bytes()).unwrap();
        } else {
            output_nodes.write_all(r#"["#.as_bytes()).unwrap();
            output_nodes.write_all(v.to_string().as_bytes()).unwrap();
            output_nodes.write_all(r#"]"#.as_bytes()).unwrap();
        }
    }
    output_nodes.write_all(r#"}"#.as_bytes()).unwrap();
    output_nodes.write_all("\n".as_bytes()).unwrap();

    loop {
        if key.eq("classes") {
            read_entities(json, output_nodes, &datasource, "ols:Class", defining_only, skip_obsolete);
        } else if key.eq("properties") {
            read_entities(json, output_nodes, &datasource, "ols:Property", defining_only, skip_obsolete);
        } else if key.eq("individuals") {
            read_entities(json, output_nodes, &datasource, "ols:Individual", defining_only, skip_obsolete);
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

fn read_entities(json: &mut JsonStreamReader<BufReader<StdinLock<'_>>>, output_nodes: &mut BufWriter<StdoutLock>, datasource:&String, grebitype:&str, defining_only:bool, skip_obsolete:bool) {
    json.begin_array().unwrap();
    while json.has_next().unwrap() {
        let mut val:Value = read_value(json);
        let obj = val.as_object_mut().unwrap();

        // eprintln!("obj: {:?}", obj);

        if defining_only {
            if obj.contains_key("ols:imported") && get_string_values(obj.get("ols:imported").unwrap()).iter().next().unwrap().eq(&"true") {
                continue
            }
        }

        if skip_obsolete {
            if obj.contains_key("ols:isObsolete") {
                if get_string_values(obj.get("ols:isObsolete").unwrap()).iter().next().unwrap().eq(&"true") {
                    continue;
                }
            }
        }

        if grebitype.eq("ols:Property") { 

            let qualified_safe_label = {
                let curie = get_string_values(obj.get("ols:curie").unwrap()).iter().next().unwrap().to_string();
                let pref_prefix = {
                    if curie.contains(":") {
                        Some(curie.split(":").next().unwrap().to_ascii_lowercase())
                    } else {
			let definedBy = obj.get("ols:definedBy");
			if definedBy.is_some() {
				Some(get_string_values(definedBy.unwrap()).iter().next().unwrap().to_string())
			} else {
				None
			}
                    }
                };
		if !pref_prefix.is_some() {
			curie.to_string()
		} else {
			let pref_prefix_u = pref_prefix.unwrap().to_string();
			let label = get_string_values(obj.get("ols:label").unwrap()).iter().next().unwrap().to_string();

			// this might not be a real label, in which case just return the curie
			if label.starts_with(&(pref_prefix_u.to_owned()  + ":")) || label.starts_with(&(pref_prefix_u.to_owned() + "_")) {
			    curie.to_string()
			} else {
			    pref_prefix_u.to_string() + ":" + &label.to_string().as_bytes().iter().map(|x| {
				if x.is_ascii_alphanumeric() {
				    *x as char
				} else {
				    '_'
				}
			    }).collect::<String>()
			}
		}
            };

            output_nodes.write_all(r#"{"id":"#.as_bytes()).unwrap();
            output_nodes.write_all(Value::String(qualified_safe_label).to_string().as_bytes()).unwrap();
        } else {
            output_nodes.write_all(r#"{"id":"#.as_bytes()).unwrap();
            let curie = get_string_values(obj.get("ols:curie").unwrap()).iter().next().unwrap().to_string();
            output_nodes.write_all(Value::String(curie).to_string().as_bytes()).unwrap();
        }

        output_nodes.write_all(r#","grebi:datasource":""#.as_bytes()).unwrap();
        output_nodes.write_all(datasource.as_bytes()).unwrap();
        output_nodes.write_all(r#"","grebi:type":[""#.as_bytes()).unwrap();
        output_nodes.write_all(grebitype.as_bytes()).unwrap();
        output_nodes.write_all(r#"""#.as_bytes()).unwrap();
        output_nodes.write_all(r#"]"#.as_bytes()).unwrap();

        for k in obj.keys() {

            if k.eq("ols:searchableAnnotationValues") {
                continue;
            }

            output_nodes.write_all(r#","#.as_bytes()).unwrap();
            output_nodes.write_all(r#"""#.as_bytes()).unwrap();
            output_nodes.write_all(k.as_bytes()).unwrap();// already reprefixed on load
            output_nodes.write_all(r#"":"#.as_bytes()).unwrap();

            let v = obj.get(k).unwrap();

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

        // hack to assert equivalence for NCBITaxon ids to species names
        // because some important DBs (including reactome, metabolights) just use names instead of IDs
        // really this should be implemented as a separate datasource using sssom string->id mappings extracted from ncbitaxon
        if obj.contains_key("ols:ontologyId") && get_string_values(obj.get("ols:ontologyId").unwrap()).iter().next().unwrap().eq(&"ncbitaxon") {
            if obj.contains_key("http://purl.obolibrary.org/obo/ncbitaxon#has_rank") && get_string_values(obj.get("http://purl.obolibrary.org/obo/ncbitaxon#has_rank").unwrap()).iter().next().unwrap().eq(&"http://purl.obolibrary.org/obo/NCBITaxon_species") {
                output_nodes.write_all(r#","grebi:equivalentTo":["#.as_bytes()).unwrap();
                let mut is_first_species_name = true;
                for name in get_string_values(obj.get("ols:label").unwrap()) {
                    if is_first_species_name {
                        is_first_species_name = false;
                    } else {
                        output_nodes.write_all(r#","#.as_bytes()).unwrap();
                    }
                    output_nodes.write_all(r#"""#.as_bytes()).unwrap();
                    write_escaped_string(&name.as_bytes(), output_nodes);
                    output_nodes.write_all(r#"""#.as_bytes()).unwrap();
                }
                output_nodes.write_all(r#"]"#.as_bytes()).unwrap();
            }
        }

        output_nodes.write_all(r#"}"#.as_bytes()).unwrap();
        output_nodes.write_all("\n".as_bytes()).unwrap();
    }
    json.end_array().unwrap();
}

fn write_value(v:&Value, output_nodes: &mut BufWriter<StdoutLock>) {
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
        let obj_types = obj.get("ols:type");

        if obj_types.is_some() {
            if has_reification_type(obj_types.unwrap().as_array().unwrap()) {
                let reified_value = obj.get("ols:value").unwrap();
                let axiom_sets = obj.get("ols:axioms").unwrap().as_array().unwrap();
                for axiom_set in axiom_sets {
                    let reified_props = axiom_set.as_object().unwrap();

                    output_nodes.write_all(r#"{"grebi:value":"#.as_bytes()).unwrap();
                    write_value(reified_value, output_nodes);
                    output_nodes.write_all(r#","grebi:properties":{"#.as_bytes()).unwrap();
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
                if obj.contains_key("ols:value") {
                    let value = obj.get("ols:value").unwrap();
                    write_value(&value, output_nodes);
                } else {
                    panic!("Unknown value: {:?}", serde_json::to_string(obj));
                }
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
        let value = v.get("ols:value");
        if value.is_some() {
            return get_string_values(value.unwrap());
        }
    }
    if v.is_array() {
        return v.as_array().unwrap().iter().map(|x| get_string_values(x)).flatten().collect();
    }
    return [].to_vec();
}
fn read_value(json: &mut JsonStreamReader<BufReader<StdinLock<'_>>>) -> Value {
    match json.peek().unwrap() {
        struson::reader::ValueType::Array => {
            let mut elems:Vec<Value> = Vec::new();
            json.begin_array().unwrap();
            while json.has_next().unwrap() {
                elems.push(read_value(json));
            }
            json.end_array().unwrap();
            return Value::Array(elems);
        }
        struson::reader::ValueType::Object => {
            let mut obj:Map<String,Value> = Map::new();
            json.begin_object().unwrap();
            while json.has_next().unwrap() {
                let k = json.next_name_owned().unwrap();
                obj.insert( reprefix_predicate(&k), read_value(json));
            }
            json.end_object().unwrap();
            return Value::Object(obj);
        }
        struson::reader::ValueType::String => {
            return Value::String(  json.next_string().unwrap().to_string() );
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

fn reprefix_predicate(pred:&str) -> String {
    if pred.contains(":") {
        return pred.to_string();
    } else {
        return "ols:".to_owned() + pred;
    }
}

fn write_escaped_string(str:&[u8], writer:&mut BufWriter<StdoutLock>) {
    for c in str {
        match c {
            b'"' => { writer.write_all(b"\\\"").unwrap(); }
            b'\\' => { writer.write_all(b"\\\\").unwrap(); }
            b'\n' => { writer.write_all(b"\\n").unwrap(); }
            b'\r' => { writer.write_all(b"\\r").unwrap(); }
            b'\t' => { writer.write_all(b"\\t").unwrap(); }
            _ => { writer.write_all([*c].as_slice()).unwrap(); }
        }
    }
}


