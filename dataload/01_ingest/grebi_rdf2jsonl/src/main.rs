use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, StdoutLock};
use std::rc::Rc;
use sophia::graph::Graph;
use sophia::graph::inmem::{SpoWrapper, GenericGraph, GraphWrapper};
use sophia::parser::xml::RdfXmlParser;
use sophia::parser::nq::NQuadsParser;
use sophia::parser::turtle::TurtleParser;
use sophia::graph::MutableGraph;
use sophia::term::factory::RcTermFactory;
use sophia_api::term::matcher::ANY;
use sophia::triple::stream::TripleSource;
use sophia::quad::stream::QuadSource;
use sophia::quad::Quad;
use serde_json::{Value, Map, json};
use sophia::triple::Triple;
use sophia::term::{SimpleIri, TTerm, Term};
use sophia::term::TermKind::{BlankNode, Iri, Literal, Variable};
use sophia::parser::TripleParser;
use sophia::parser::QuadParser;
use std::io::Write;
use clap::Parser;

const RDF_TYPE:SimpleIri<'static> =
    SimpleIri::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#", Some("type"));

const RDF_STATEMENT:SimpleIri<'static> =
    SimpleIri::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#", Some("Statement"));
const RDF_SUBJECT:SimpleIri<'static> =
    SimpleIri::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#", Some("subject"));
const RDF_PREDICATE:SimpleIri<'static> =
    SimpleIri::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#", Some("predicate"));
const RDF_OBJECT:SimpleIri<'static> =
    SimpleIri::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#", Some("object"));

const OWL_AXIOM:SimpleIri<'static> =
    SimpleIri::new_unchecked("http://www.w3.org/2002/07/owl#", Some("Axiom"));
const RDFS_ISDEFINEDBY:SimpleIri<'static> =
    SimpleIri::new_unchecked("http://www.w3.org/2000/01/rdf-schema#", Some("isDefinedBy"));
const OWL_SUBJECT:SimpleIri<'static> =
    SimpleIri::new_unchecked("http://www.w3.org/2002/07/owl#", Some("annotatedSource"));
const OWL_PREDICATE:SimpleIri<'static> =
    SimpleIri::new_unchecked("http://www.w3.org/2002/07/owl#", Some("annotatedProperty"));
const OWL_OBJECT:SimpleIri<'static> = 
    SimpleIri::new_unchecked("http://www.w3.org/2002/07/owl#", Some("annotatedTarget"));

// #[global_allocator] 
// static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;


#[derive(Eq, Hash, PartialEq)]
struct ReifLhs{
    s: Term<Rc<str>>,
    p: Term<Rc<str>>,
}

type CustomGraph = SpoWrapper<GenericGraph<u32, RcTermFactory>>;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    rdf_type: String, // so far: "rdf_triples_xml" or "rdf_quads_nq"
    
    #[arg(long, num_args(0..))]
    rdf_graph:Vec<String>, // named graphs to load, if we are loading quads

    #[arg(long)]
    nest_objects_of_predicate:Vec<String>,

    #[arg(long)]
    exclude_objects_of_predicate:Vec<String>, // if an object is used with this predicate, ignore the object

    #[arg(long)]
    reif_pointer_predicate:Vec<String>, // predicates pointing to a reification metadata object

    #[arg(long)]
    reif_predicate_predicate:Vec<String>, // predicates from reification metadata object to the predicate

    #[arg(long)]
    reif_value_predicate:Vec<String>, // predicates from reification metadata object to the actual value

    #[arg(long, default_value_t = false)]
    rdf_types_are_grebi_types:bool,

    #[arg(long)]
    map_predicate:Vec<String>, // rename a predicate to a new JSON key, FROM=TO (e.g. "http://www.w3.org/2000/01/rdf-schema#subClassOf=biolink:broad_match")

    #[arg(long)]
    datasource_from_isdefinedby:Option<String> // JSON file mapping ontology IRI -> datasource name; per term, set grebi:datasource from its rdfs:isDefinedBy target (per-ontology provenance for the ubergraph)
}

fn main() -> std::io::Result<()> {

     let args = Args::parse();

    let start_time = std::time::Instant::now();

    // Read RDF/XML from stdin
    let stdin = io::stdin();
    let handle = stdin.lock();
    let reader = BufReader::new(handle);

    let stdout = io::stdout().lock();
    let mut output_nodes = BufWriter::new(stdout);

    let nest_preds:BTreeSet<String> = args.nest_objects_of_predicate.into_iter().collect();
    let ignore_preds:BTreeSet<String> = args.exclude_objects_of_predicate.into_iter().collect();
    let reif_pointer_preds:BTreeSet<String> = args.reif_pointer_predicate.into_iter().collect();
    let reif_value_preds:BTreeSet<String> = args.reif_value_predicate.into_iter().collect();
    let rdf_types_are_grebi_types = args.rdf_types_are_grebi_types;

    // Per-ingest predicate renames (FROM=TO). Used to emit the ubergraph's
    // redundant vs non-redundant rdfs:subClassOf as distinct biolink predicates
    // (broad_match vs subclass_of) so the closure is queryable separately.
    let map_predicate:Vec<(String,String)> = args.map_predicate.iter().map(|m| {
        let eq = m.find('=').expect("--map-predicate must be FROM=TO");
        (m[0..eq].to_string(), m[eq+1..].to_string())
    }).collect();

    let gr:CustomGraph = match args.rdf_type.as_str() {
        "rdf_triples_xml" => {
            let parser = RdfXmlParser { base: Some("http://www.ebi.ac.uk/kg/".into()) };
            let g:CustomGraph = parser.parse(reader).collect_triples::<CustomGraph>().unwrap();
            Ok::<CustomGraph, io::Error>(g)
        },
        "rdf_triples_turtle" => {
            let parser = TurtleParser { base: Some("http://www.ebi.ac.uk/kg/".into()) };
            let g:CustomGraph = parser.parse(reader).collect_triples::<CustomGraph>().unwrap();
            Ok::<CustomGraph, io::Error>(g)
        },
        "rdf_quads_nq" => {

            let parser = NQuadsParser {};
            
            let quad_source = parser.parse(reader);
            let mut filtered_quads = quad_source.filter_quads(|q|
                args.rdf_graph.len() == 0 || args.rdf_graph.contains(&q.g().unwrap().value().to_string()));

            let mut g:CustomGraph = CustomGraph::new();

            // TODO: can't figure out how to stream the quad graph as triples
            // so this will have to do for now...
            //
            filtered_quads.for_each_quad(|q| {
                g.insert(q.s(), q.p(), q.o()).unwrap();
            }).unwrap();

            Ok::<CustomGraph, io::Error>(g)
        },
        _ => { panic!("unknown datasource type"); }
    }.unwrap();

    let ds = gr.as_dataset().unwrap();

    eprintln!("Loading graph took {} seconds", start_time.elapsed().as_secs());


    let mut exclude_subjects_at_toplevel = HashSet::new();
    let mut exclude_subjects:HashSet<Term<Rc<str>>> = HashSet::new();

    let mut owl_axiom_subjs:Vec<Term<Rc<str>>> = Vec::new();
    let mut rdf_statement_subjs:Vec<Term<Rc<str>>> = Vec::new();

    // Per-ontology provenance: map each ontology IRI (the rdfs:isDefinedBy target
    // owlmake stamps on every term) to a datasource name (e.g. Ontologies.efo).
    // For the ubergraph ontology graph this lets a single ingest tag each term
    // with the ontology it actually came from.
    let isdefinedby_iri_to_ds:HashMap<String,String> = match &args.datasource_from_isdefinedby {
        Some(path) => serde_json::from_reader(BufReader::new(File::open(path).unwrap())).unwrap(),
        None => HashMap::new(),
    };
    let mut subject_to_datasource:HashMap<String,String> = HashMap::new();

    for triple in ds.triples() {
        let triple_u = triple.unwrap();
        if triple_u.p().eq(&RDF_TYPE) {
            if triple_u.o().eq(&OWL_AXIOM) {
                owl_axiom_subjs.push(triple_u.s().clone());
            } else if triple_u.o().eq(&RDF_STATEMENT) {
                rdf_statement_subjs.push(triple_u.s().clone());
            }
        }
        if !isdefinedby_iri_to_ds.is_empty() && triple_u.p().eq(&RDFS_ISDEFINEDBY) {
            if let Some(ds_name) = isdefinedby_iri_to_ds.get(&triple_u.o().value().to_string()) {
                subject_to_datasource.insert(triple_u.s().value().to_string(), ds_name.clone());
            }
        }
        if nest_preds.contains(&triple_u.p().value().to_string()) {
            exclude_subjects_at_toplevel.insert(triple_u.o().clone());
        }
        if ignore_preds.contains(&triple_u.p().value().to_string())
                || reif_pointer_preds.contains(&triple_u.p().value().to_string())  {
            exclude_subjects.insert(triple_u.o().clone());
        }
    }
    eprintln!("Found {} owl axioms and {} rdf statements", owl_axiom_subjs.len(), rdf_statement_subjs.len());

    let mut reifs:HashMap<ReifLhs, BTreeMap<String, Term<Rc<str>>>> = HashMap::new();
    populate_reifs(&mut reifs, rdf_statement_subjs, RDF_SUBJECT, RDF_PREDICATE, RDF_OBJECT, ds, &nest_preds, &exclude_subjects, &reif_pointer_preds, &reif_value_preds);
    populate_reifs(&mut reifs, owl_axiom_subjs, OWL_SUBJECT, OWL_PREDICATE, OWL_OBJECT, ds, &nest_preds, &exclude_subjects, &reif_pointer_preds, &reif_value_preds);

    eprintln!("Building reification index took {} seconds", start_time.elapsed().as_secs());

    write_subjects(ds, &mut output_nodes, &nest_preds, &exclude_subjects, &exclude_subjects_at_toplevel, reifs, rdf_types_are_grebi_types, &reif_pointer_preds, &reif_value_preds, &map_predicate, &subject_to_datasource);

    eprintln!("Total time elapsed: {} seconds", start_time.elapsed().as_secs());

    Ok(())
}

fn populate_reifs(
    reifs:&mut HashMap<ReifLhs, BTreeMap<String, Term<Rc<str>>>>,
    subjs:Vec<Term<Rc<str>>>,
    subj_prop:SimpleIri,
    pred_prop:SimpleIri,
    obj_prop:SimpleIri,
    ds:&CustomGraph,
    nest_preds:&BTreeSet<String>,
    exclude_subjects:&HashSet<Term<Rc<str>>>,
    reif_pointer_preds:&BTreeSet<String>,
    reif_value_preds:&BTreeSet<String>
) {

    for s in subjs {

        let annotated_subject = ds.triples_matching(&s, &subj_prop, &ANY).next().unwrap().unwrap().o().clone();

        if exclude_subjects.contains(&annotated_subject) {
            continue;
        }

        let annotated_predicate = ds.triples_matching(&s, &pred_prop, &ANY).next().unwrap().unwrap().o().clone();
        let annotated_object = ds.triples_matching(&s, &obj_prop, &ANY).next().unwrap().unwrap().o().clone();

        let obj_json = term_to_json(&annotated_object, ds, nest_preds, None, false, reif_pointer_preds, reif_value_preds).to_string();

        let lhs =  ReifLhs {
            s: annotated_subject.clone(),
            p: annotated_predicate.clone()
        };

        let obj_to_reif = reifs.get_mut(&lhs);
        
        if obj_to_reif.is_some() {
            let obj_to_reif_u = obj_to_reif.unwrap();
            obj_to_reif_u.insert(obj_json, s.clone());
        } else {
            let mut obj_to_reif_u = BTreeMap::new();
            obj_to_reif_u.insert(obj_json, s.clone());
            reifs.insert(lhs, obj_to_reif_u);
        }
    }


}


fn write_subjects(
    ds:&CustomGraph,
    nodes_writer:&mut BufWriter<StdoutLock>,
    nest_preds:&BTreeSet<String>,
    exclude_subjects:&HashSet<Term<Rc<str>>>,
    exclude_subjects_at_toplevel:&HashSet<Term<Rc<str>>>,
    reifs:HashMap<ReifLhs, BTreeMap<String, Term<Rc<str>>>>,
    rdf_types_are_grebi_types:bool,
    reif_pointer_preds:&BTreeSet<String>,
    reif_value_preds:&BTreeSet<String>,
    map_predicate:&[(String,String)],
    subject_to_datasource:&HashMap<String,String>,
) {

    let start_time2 = std::time::Instant::now();

    'write_subjs: for s in &ds.gw_subjects().unwrap() {

        if s.kind() != Iri {
            continue; 
        }

        if exclude_subjects.contains(s) {
            continue;
        }
        if exclude_subjects_at_toplevel.contains(s) {
            continue;
        }

        let mut json = term_to_json(s, ds, nest_preds, Some(&reifs), rdf_types_are_grebi_types, reif_pointer_preds, reif_value_preds);

        let json_obj = json.as_object().unwrap();
        let types = json_obj.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        if types.is_some() {
            for t in types.unwrap().as_array().unwrap() {
                if t.is_string() {
                    if t.as_str().unwrap().eq("http://www.w3.org/2002/07/owl#Axiom")
                        || t.as_str().unwrap().eq("http://www.w3.org/1999/02/22-rdf-syntax-ns#Statement")
                    {
                        continue 'write_subjs;
                    }
                }
            }
        }

        // Rename top-level predicate keys per --map-predicate (e.g. the
        // materialised relation graphs' rdfs:subClassOf -> biolink:broad_match /
        // biolink:subclass_of). Values are always arrays here; merge if the
        // target key already exists.
        if !map_predicate.is_empty() {
            let obj = json.as_object_mut().unwrap();
            for (from, to) in map_predicate {
                if let Some(v) = obj.remove(from.as_str()) {
                    match obj.get_mut(to.as_str()) {
                        Some(existing) => {
                            if let (Some(ex), Some(add)) = (existing.as_array_mut(), v.as_array()) {
                                ex.extend(add.iter().cloned());
                            }
                        }
                        None => { obj.insert(to.clone(), v); }
                    }
                }
            }
        }

        // Per-ontology provenance: tag this term with the datasource derived from
        // its rdfs:isDefinedBy target (e.g. Ontologies.efo). grebi_merge honours a
        // per-line grebi:datasource, overriding the ingest's file-level id.
        if !subject_to_datasource.is_empty() {
            if let Some(ds_name) = subject_to_datasource.get(&s.value().to_string()) {
                json.as_object_mut().unwrap().insert("grebi:datasource".to_string(), Value::String(ds_name.clone()));
            }
        }

        nodes_writer.write_all(json.to_string().as_bytes()).unwrap();
        nodes_writer.write_all("\n".as_bytes()).unwrap();
    }

    eprintln!("Writing JSONL took {} seconds", start_time2.elapsed().as_secs());
}

fn term_to_json(
    term:&Term<Rc<str>>,
    ds:&CustomGraph,
    nest_preds:&BTreeSet<String>,
    reifs:Option<&HashMap<ReifLhs, BTreeMap<String, Term<Rc<str>>>>>,
    rdf_types_are_grebi_types:bool,
    reif_pointer_preds:&BTreeSet<String>,
    reif_value_preds:&BTreeSet<String>
) -> Value {

    let triples = ds.triples_matching(term, &ANY, &ANY);

    let mut json:Map<String,Value> = Map::new();

    if term.kind() == Iri {
        json.insert("id".to_string(), Value::String(term.value().to_string()));
    }

    for t in triples {

        let tu = t.unwrap();

        let tu_p = tu.p();

        // when we serialize a reification, don't need the reified s/p/o anymore
        if tu_p.eq(&RDF_SUBJECT) || tu_p.eq(&RDF_PREDICATE) || tu_p.eq(&RDF_OBJECT)
            || tu_p.eq(&OWL_SUBJECT) || tu_p.eq(&OWL_PREDICATE) || tu_p.eq(&OWL_OBJECT) {
            continue;
        }

        let p_iri = tu_p.value().to_string();
        let p = &p_iri;
        let o = tu.o();

        // is this a predicate pointing to a reification metadata object?
        if reif_pointer_preds.contains(p) {

            let mut reif_metadata_obj = match o.kind() {
                Iri|Literal|BlankNode => {
                    let mut obj = term_to_json(o, ds, nest_preds, reifs, false, reif_pointer_preds, reif_value_preds);
                    let obj_o = obj.as_object_mut().unwrap();
                    obj_o.remove_entry("id");
                    obj
                },
                Variable => todo!(),
            };

            let mut reif_metadata_obj_as_json_o = reif_metadata_obj.as_object_mut().unwrap();
            reif_metadata_obj_as_json_o.remove_entry("id");

            let actual_predicate = reif_metadata_obj_as_json_o.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap().as_array().unwrap().get(0).unwrap().as_str().unwrap().to_string();
            reif_metadata_obj_as_json_o.remove_entry("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

            let value_p = reif_value_preds.iter().filter(|&vp| {
                reif_metadata_obj_as_json_o.contains_key(vp)
            }).next().unwrap().to_owned();

            let actual_value = reif_metadata_obj_as_json_o.get(&value_p).unwrap().to_owned();
            reif_metadata_obj_as_json_o.remove_entry(&value_p);

            let v = json!({
                "grebi:value": actual_value,
                "grebi:properties": reif_metadata_obj_as_json_o
            });

            let existing = json.get_mut(&actual_predicate);

            if existing.is_some() {
                existing.unwrap().as_array_mut().unwrap().push(v);
            } else {
                json.insert(actual_predicate.to_string(), json!([ v ]));
            }
            
        } else {

            let reif_subj = {
                if reifs.is_some() {
                    let reifs_u = reifs.unwrap();
                    let reifs_for_this_sp = reifs_u.get(&ReifLhs { s: tu.s().clone(), p: tu.p().clone() });
                    if reifs_for_this_sp.is_some() {
                        let reifs_for_this_sp_u = reifs_for_this_sp.unwrap();
                        let o_json = term_to_json(&o, ds, nest_preds, None, false, reif_pointer_preds, reif_value_preds).to_string();
                        let reif = reifs_for_this_sp_u.get(&o_json);
                        if reif.is_some() {
                            Some(reif.unwrap())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            };

            let mut v = {
                if nest_preds.contains(p) {
                    match o.kind() {
                        Iri|Literal|BlankNode => {
                            let mut obj = term_to_json(o, ds, nest_preds, reifs, false, reif_pointer_preds, reif_value_preds);
                            let obj_o = obj.as_object_mut().unwrap();
                            obj_o.remove_entry("id");
                            obj
                        },
                        Variable => todo!(),
                    }
                } else {
                    match o.kind() {
                        Iri|Literal => Value::String( o.value().to_string() ),
                        BlankNode => term_to_json(o, ds, nest_preds, reifs, false, reif_pointer_preds, reif_value_preds),
                        Variable => todo!(),
                    }
                }
            };

            if reif_subj.is_some() {
                let mut reif_as_json = term_to_json(reif_subj.unwrap(), ds, nest_preds, None, false, reif_pointer_preds, reif_value_preds);
                let reif_as_json_o = reif_as_json.as_object_mut().unwrap();
                reif_as_json_o.remove_entry("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
                reif_as_json_o.remove_entry("id");
                v = json!({
                    "grebi:value": v,
                    "grebi:properties": reif_as_json_o
                })
            }

            let existing = json.get_mut(&p_iri);

            if existing.is_some() {
                existing.unwrap().as_array_mut().unwrap().push(v);
            } else {
                json.insert(p_iri, json!([ v ]));
            }
        }
    }

    if rdf_types_are_grebi_types && json.contains_key("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") {
        let types = json.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap().as_array().unwrap();
        json.insert("grebi:type".to_string(), Value::Array(
            types.iter().filter(|t| t.is_string()).map(|t| {
                Value::String(t.as_str().unwrap().to_string())
            }).collect()
        ));
    }
    
    return Value::Object(json);
}





