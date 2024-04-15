use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Write};
use std::rc::Rc;
use serde_json::{Map, Value};
use sophia_api::ns::{owl, rdf};
use sophia_api::term::matcher::Any;
use sophia_api::term::{SimpleTerm, TermKind};
use sophia_api::graph::{Graph, MutableGraph};
use sophia_api::prelude::Iri;
use clap::Parser;
use serde_json::json;

use grebi_shared::prefix_map::PrefixMap;
use grebi_shared::prefix_map::PrefixMapBuilder;
use grebi_shared::serialize_equivalence;
use sophia_inmem::graph::{FastGraph, GenericLightGraph, LightGraph};
use sophia_xml::parser::RdfXmlParser;
use sophia_api::parser::TripleParser;
use sophia_api::parser::QuadParser;
use sophia_api::prelude::TripleSource;
use sophia_api::prelude::Triple;
use sophia_api::prelude::QuadSource;
use sophia_api::prelude::Quad;
use sophia_api::prelude::Term;
use sophia_turtle::parser::nq::NQuadsParser;
use multimap::MultiMap;

// #[global_allocator] 
// static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

type ReifIndex = MultiMap<(SimpleTerm<'static>,SimpleTerm<'static>), SimpleTerm<'static>>;
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

    #[arg(short, long)]
    output_nodes:String,

    #[arg(short, long)]
    output_equivalences:String
}

fn main() -> std::io::Result<()> {

     let args = Args::parse();

    eprintln!("grebi_ingest_rdf running for {}", args.datasource_name);


    let normalise = {
        let rdr = BufReader::new( std::fs::File::open("prefix_map_normalise.json").unwrap() );
        let mut builder = PrefixMapBuilder::new();
        serde_json::from_reader::<_, HashMap<String, String>>(rdr).unwrap().into_iter().for_each(|(k, v)| {
            builder.add_mapping(k, v);
        });
        builder.build()
    };


    let start_time = std::time::Instant::now();

    // Read RDF/XML from stdin
    let stdin = io::stdin();
    let handle = stdin.lock();
    let reader = BufReader::new(handle);

    let mut output_nodes = BufWriter::new(
        File::create(args.output_nodes.as_str()).unwrap());

    let mut output_equivalences = BufWriter::new(
         File::create(args.output_equivalences.as_str()).unwrap()
    );
    // output_equivalences.write_all(b"subject_id\tobject_id\n").unwrap();

        
    let gr:LightGraph = match args.rdf_type.as_str() {
        "rdf_triples_xml" => {
            let parser = RdfXmlParser { base: Some(Iri::new("http://www.ebi.ac.uk/kg/".to_string()).unwrap()) };
            let g:LightGraph = parser.parse(reader).collect_triples::<LightGraph>().unwrap();
            Ok::<LightGraph, io::Error>(g)
        },
        "rdf_quads_nq" => {

            if args.rdf_graph.len() == 0 {
                panic!("must specify at least one graph to load for nquads");
            }

            let parser = NQuadsParser {};
            
            let quad_source = parser.parse(reader);
            let mut filtered_quads = quad_source.filter_quads(|q| args.rdf_graph.contains(&q.g().unwrap().to_string()));

            let mut g:LightGraph = LightGraph::new();

            // TODO: can't figure out how to stream the quad graph as triples
            // so this will have to do for now...
            //
            filtered_quads.for_each_quad(|q| {
                g.insert(q.s(), q.p(), q.o()).unwrap();
            }).unwrap();


            Ok::<LightGraph, io::Error>(g)
        },
        _ => { panic!("unknown datasource type"); }
    }.unwrap();

    let ds = gr.as_dataset().unwrap();

    eprintln!("Loading graph took {} seconds", start_time.elapsed().as_secs());

    let start_time2 = std::time::Instant::now();

    let mut reif:ReifIndex = ReifIndex::new();

    for triple in ds.triples() {
        let t = triple.unwrap();
        let p = t.p().iri().unwrap().to_string();
        if p == "http://www.w3.org/2002/07/owl#annotatedSource" {
            let annotated_subject = t.o().clone();
            let annotated_predicate = gr.triples_matching([t.s()], ["http://www.w3.org/2002/07/owl#annotatedProperty"], Any).next().unwrap().unwrap().o().clone();
            reif.insert((annotated_subject, annotated_predicate), t.s().clone());
        }
    }
    eprintln!("Building reification index took {} seconds", start_time2.elapsed().as_secs());

    write_subjects(ds, &mut output_nodes, &mut output_equivalences, &args, &normalise, &reif);

    eprintln!("Total time elapsed: {} seconds", start_time.elapsed().as_secs());

    Ok(())
}



fn write_subjects(ds:&LightGraph, nodes_writer:&mut BufWriter<File>, equivalences_writer:&mut BufWriter<File>, args:&Args, normalise: &PrefixMap, reif: &ReifIndex) {

    let start_time2 = std::time::Instant::now();

    let middle_json_fragment
         = [r#"","datasource":""#.as_bytes(), args.datasource_name.as_bytes(), r#"","properties":"#.as_bytes() ].concat();

    for s in ds.subjects() {

        let su = s.unwrap();

        if !su.is_iri() {
            continue; 
        }

        nodes_writer.write_all(r#"{"subject":""#.as_bytes()).unwrap();
        nodes_writer.write_all( normalise.reprefix(& su.iri().unwrap().as_str().to_string() ).as_bytes()).unwrap();
        nodes_writer.write_all(&middle_json_fragment).unwrap();
        nodes_writer.write_all( term_to_json(su, ds, &normalise, equivalences_writer, reif).to_string().as_bytes()).unwrap();
        nodes_writer.write_all("}\n".as_bytes()).unwrap();
    }

    eprintln!("Writing JSONL took {} seconds", start_time2.elapsed().as_secs());
}

const EQUIV_PREDICATES :[&str;3]= [
    "http://www.w3.org/2002/07/owl#equivalentClass",
    "http://www.w3.org/2002/07/owl#equivalentProperty",
    "http://www.w3.org/2002/07/owl#sameAs"
    // "http://www.w3.org/2004/02/skos/core#exactMatch",
    // "http://www.geneontology.org/formats/oboInOwl#hasAlternativeId",
    // "http://purl.uniprot.org/uniprot/replaces",
    // "http://purl.obolibrary.org/obo/IAO_0100001" // -> replacement term
];

fn term_to_json(term:&SimpleTerm, ds:&LightGraph, normalise:&PrefixMap, equivalences_writer:&mut BufWriter<File>, reif:&ReifIndex) -> Value {

    let triples = ds.triples_matching([term.as_simple()], Any, Any);

    let mut json:Map<String,Value> = Map::new();



    for t in triples {

        let tu = t.unwrap();
        let p_iri = tu.p().iri().unwrap().as_str().to_string();
        let p = normalise.reprefix(&p_iri);

        let o = tu.o();

        let mut v = 'to_str: {
            if o.is_literal() {
                let o_compact = normalise.reprefix(&o.lexical_form().unwrap().to_string());
                if EQUIV_PREDICATES.contains(&p_iri.as_str()) {

                        let equiv = serialize_equivalence(
                            normalise.reprefix(&tu.s().iri().unwrap().to_string()).as_bytes(), o_compact.as_bytes());

                        if equiv.is_some() {
                            equivalences_writer.write_all(equiv.unwrap().as_slice()).unwrap();
                        }
                }
                break 'to_str Value::String( o_compact );
            }
            if o.is_iri() {
                let o_compact = normalise.reprefix(&o.iri().unwrap().to_string());
                if EQUIV_PREDICATES.contains(&p_iri.as_str()) {

                        let equiv = serialize_equivalence(
                            normalise.reprefix(&tu.s().iri().unwrap().to_string()).as_bytes(), o_compact.as_bytes());

                        if equiv.is_some() {
                            equivalences_writer.write_all(equiv.unwrap().as_slice()).unwrap();
                        }
                }
                break 'to_str Value::String( o_compact );
            }
            if o.is_blank_node() {
                break 'to_str term_to_json(o, ds, normalise, equivalences_writer, reif);
            }
            todo!()
        };

        // TODO why do we need to clone here
        let reifs = reif.get_vec(&(tu.s().clone(), tu.p().clone()));

        if reifs.is_some() {

            let r = reifs.unwrap();
            let mut reif_props:Map<String,Value> = Map::new();

            for reif_id in r {
                let reif_triples = ds.triples_matching([reif_id], Any, Any);
                // let annotated_object = reif_triples.filter(|t| t.p().eq("http://www.w3.org/2002/07/owl#annotatedTarget")).next().unwrap().unwrap().o().clone();
                // if is_isomorphic(&annotated_object, &o, ds) {
                // }
            }

            // v = json!({
            //     "value": v,
            //     "properties": Value::Object(reif_props)
            // })
        }

        if !term.is_blank_node() && p.eq("rdf:type") {
            // copy into grebi:type field
            let gr_existing = json.get_mut("grebi:type");
            if gr_existing.is_some() {
                gr_existing.unwrap().as_array_mut().unwrap().push(v.clone());
            } else {
                json.insert("grebi:type".to_owned(), json!([ v.clone() ]));
            }
        }

        let existing = json.get_mut(&p);
        if existing.is_some() {
            existing.unwrap().as_array_mut().unwrap().push(v);
        } else {
            json.insert(p, json!([ v ]));
        }
    }

    return Value::Object(json);
}

fn is_isomorphic(a:SimpleTerm, b:SimpleTerm, gr:&LightGraph) -> bool {

    return false;
}




