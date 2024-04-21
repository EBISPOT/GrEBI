use std::collections::HashMap;
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

// const RDF_TYPE:SimpleIri<'static> =
//     SimpleIri::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#", Some("type")).unwrap();

// const OWL_AXIOM:SimpleIri<'static> = 
//     SimpleIri::new("http://www.w3.org/2002/07/owl#", Some("Axiom")).unwrap();


use grebi_shared::prefix_map::PrefixMap;
use grebi_shared::prefix_map::PrefixMapBuilder;
use grebi_shared::serialize_equivalence;

// #[global_allocator] 
// static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

type CustomGraph = SpoWrapper<GenericGraph<u32, RcTermFactory>>;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(short, long)]
    datasource_name: String,

    #[arg(short, long, default_value_t = String::new())]
    filename: String, // we ignore this in the RDF ingest

    #[arg(short, long)]
    rdf_type: String, // so far: "rdf_triples_xml" or "rdf_quads_nq"
    
    #[arg(short, long, num_args(0..))]
    rdf_graph:Vec<String>, // named graphs to load, if we are loading quads
}

fn main() -> std::io::Result<()> {

     let args = Args::parse();

    eprintln!("grebi_ingest_rdf running for {}", args.datasource_name);

    let start_time = std::time::Instant::now();

    // Read RDF/XML from stdin
    let stdin = io::stdin();
    let handle = stdin.lock();
    let reader = BufReader::new(handle);

    let stdout = io::stdout().lock();
    let mut output_nodes = BufWriter::new(stdout);

        
    let gr:CustomGraph = match args.rdf_type.as_str() {
        "rdf_triples_xml" => {
            let parser = RdfXmlParser { base: Some("http://www.ebi.ac.uk/kg/".into()) };
            let g:CustomGraph = parser.parse(reader).collect_triples::<CustomGraph>().unwrap();
            Ok::<CustomGraph, io::Error>(g)
        },
        "rdf_quads_nq" => {

            if args.rdf_graph.len() == 0 {
                panic!("must specify at least one graph to load for nquads");
            }

            let parser = NQuadsParser {};
            
            let quad_source = parser.parse(reader);
            let mut filtered_quads = quad_source.filter_quads(|q| args.rdf_graph.contains(&q.g().unwrap().value().to_string()));

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

    // for reif in ds.gw_triples_with_po(&RDF_TYPE, &OWL_AXIOM) {
    //     let reif_u = reif.unwrap();
    // }

    write_subjects(ds, &mut output_nodes, &args);

    eprintln!("Total time elapsed: {} seconds", start_time.elapsed().as_secs());

    Ok(())
}


fn write_subjects(ds:&CustomGraph, nodes_writer:&mut BufWriter<StdoutLock>, args:&Args) {

    let start_time2 = std::time::Instant::now();

    for s in &ds.gw_subjects().unwrap() {

        if s.kind() != Iri {
            continue; 
        }

        nodes_writer.write_all( term_to_json(s, ds).to_string().as_bytes()).unwrap();
        nodes_writer.write_all("\n".as_bytes()).unwrap();
    }

    eprintln!("Writing JSONL took {} seconds", start_time2.elapsed().as_secs());
}

fn term_to_json(term:&Term<Rc<str>>, ds:&CustomGraph) -> Value {

    let triples = ds.triples_matching(term, &ANY, &ANY);

    let mut json:Map<String,Value> = Map::new();
    json.insert("id".to_string(), Value::String(term.value().to_string()));

    for t in triples {

        let tu = t.unwrap();
        let p_iri = tu.p().value().to_string();
        let p = &p_iri;

        let o = tu.o();



        let v = match o.kind() {
            //Iri => json!({ "type": "iri", "value": o.value().to_string() }),
            Iri|Literal => {
                Value::String( o.value().to_string() )
            }
            // Literal => json!({ "type": "literal", "value": o.value().to_string() }),
            BlankNode => term_to_json(o, ds),
            Variable => todo!(),
        };

        let existing = json.get_mut(&p_iri);

        if existing.is_some() {
            existing.unwrap().as_array_mut().unwrap().push(v);
        } else {
            json.insert(p_iri, json!([ v ]));
        }
    }

    return Value::Object(json);
}





