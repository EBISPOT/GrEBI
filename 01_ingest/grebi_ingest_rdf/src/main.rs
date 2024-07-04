use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, StdoutLock};
use std::rc::Rc;
use sophia::graph::Graph;
use sophia::graph::inmem::{SpoWrapper, GenericGraph, GraphWrapper};
use sophia::parser::xml::RdfXmlParser;
use sophia::parser::nq::NQuadsParser;
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
    datasource_name: Option<String>,

    #[arg(long, default_value_t = String::new())]
    filename: String, // we ignore this in the RDF ingest

    #[arg(long)]
    rdf_type: String, // so far: "rdf_triples_xml" or "rdf_quads_nq"
    
    #[arg(long, num_args(0..))]
    rdf_graph:Vec<String>, // named graphs to load, if we are loading quads

    #[arg(long)]
    nest_objects_of_predicate:Vec<String>,

    #[arg(long)]
    exclude_objects_of_predicate:Vec<String>, // if an object is used with this predicate, ignore the object

    #[arg(long, default_value_t = false)]
    rdf_types_are_grebi_types:bool 
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
    let rdf_types_are_grebi_types = args.rdf_types_are_grebi_types;
        
    let gr:CustomGraph = match args.rdf_type.as_str() {
        "rdf_triples_xml" => {
            let parser = RdfXmlParser { base: Some("http://www.ebi.ac.uk/kg/".into()) };
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

    for triple in ds.triples() {
        let triple_u = triple.unwrap();
        if triple_u.p().eq(&RDF_TYPE) {
            if triple_u.o().eq(&OWL_AXIOM) {
                owl_axiom_subjs.push(triple_u.s().clone());
            } else if triple_u.o().eq(&RDF_STATEMENT) {
                rdf_statement_subjs.push(triple_u.s().clone());
            }
        }
        if nest_preds.contains(&triple_u.p().value().to_string()) {
            exclude_subjects_at_toplevel.insert(triple_u.o().clone());
        }
        if ignore_preds.contains(&triple_u.p().value().to_string()) {
            exclude_subjects.insert(triple_u.o().clone());
        }
    }
    eprintln!("Found {} owl axioms and {} rdf statements", owl_axiom_subjs.len(), rdf_statement_subjs.len());

    let mut reifs:HashMap<ReifLhs, BTreeMap<String, Term<Rc<str>>>> = HashMap::new();
    populate_reifs(&mut reifs, rdf_statement_subjs, RDF_SUBJECT, RDF_PREDICATE, RDF_OBJECT, ds, &nest_preds, &exclude_subjects);
    populate_reifs(&mut reifs, owl_axiom_subjs, OWL_SUBJECT, OWL_PREDICATE, OWL_OBJECT, ds, &nest_preds, &exclude_subjects);

    eprintln!("Building reification index took {} seconds", start_time.elapsed().as_secs());

    write_subjects(ds, &mut output_nodes, &nest_preds, &exclude_subjects, &exclude_subjects_at_toplevel, reifs, rdf_types_are_grebi_types);

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
    exclude_subjects:&HashSet<Term<Rc<str>>>
) {

    for s in subjs {

        let annotated_subject = ds.triples_matching(&s, &subj_prop, &ANY).next().unwrap().unwrap().o().clone();

        if exclude_subjects.contains(&annotated_subject) {
            continue;
        }

        let annotated_predicate = ds.triples_matching(&s, &pred_prop, &ANY).next().unwrap().unwrap().o().clone();
        let annotated_object = ds.triples_matching(&s, &obj_prop, &ANY).next().unwrap().unwrap().o().clone();

        let obj_json = term_to_json(&annotated_object, ds, nest_preds, None, false).to_string();

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
    rdf_types_are_grebi_types:bool) {

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

        let json = term_to_json(s, ds, nest_preds, Some(&reifs), rdf_types_are_grebi_types);

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
    rdf_types_are_grebi_types:bool
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

        let reif_subj = {
            if reifs.is_some() {
                let reifs_u = reifs.unwrap();
                let reifs_for_this_sp = reifs_u.get(&ReifLhs { s: tu.s().clone(), p: tu.p().clone() });
                if reifs_for_this_sp.is_some() {
                    let reifs_for_this_sp_u = reifs_for_this_sp.unwrap();
                    let o_json = term_to_json(&o, ds, nest_preds, None, false).to_string();
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
                        let mut obj = term_to_json(o, ds, nest_preds, reifs, false);
                        let obj_o = obj.as_object_mut().unwrap();
                        obj_o.remove_entry("id");
                        obj
                    },
                    Variable => todo!(),
                }
            } else {
                match o.kind() {
                    Iri|Literal => Value::String( o.value().to_string() ),
                    BlankNode => term_to_json(o, ds, nest_preds, reifs, false),
                    Variable => todo!(),
                }
            }
        };

        if reif_subj.is_some() {
            let mut reif_as_json = term_to_json(reif_subj.unwrap(), ds, nest_preds, None, false);
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

    if rdf_types_are_grebi_types && json.contains_key("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") {
        json.insert("grebi:type".to_string(), json.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap().clone());
    }
    
    return Value::Object(json);
}





