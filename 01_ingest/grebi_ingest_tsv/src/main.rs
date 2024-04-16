
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, self, BufReader, StdinLock, StdoutLock, Write, BufRead, Read};
use std::ptr::eq;
use clap::Parser;
use grebi_shared::prefix_map::PrefixMap;
use grebi_shared::prefix_map::PrefixMapBuilder;
use grebi_shared::serialize_equivalence;
use serde_json::json;
use serde_yaml;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    datasource_name: String,

    #[arg(long)]
    filename: String,

    #[arg(long)]
    csv_subject_field:String

    #[arg(long)]
    csv_inject_type:String

    #[arg(long)]
    csv_inject_prefix:String
}

fn main() {

    let args = Args::parse();

    let stdin = io::stdin().lock();

    let datasource_name = args.datasource_name.as_str();

    let stdout = io::stdout().lock();
    let mut output_nodes = BufWriter::new(stdout);

    let normalise:PrefixMap = {
        let rdr = BufReader::new( std::fs::File::open("prefix_map_normalise.json").unwrap() );
        let mut builder = PrefixMapBuilder::new();
        serde_json::from_reader::<_, HashMap<String, String>>(rdr).unwrap().into_iter().for_each(|(k, v)| {
            builder.add_mapping(k, v);
        });
        builder.build()
    };

    let mut csv_reader =
        csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .flexible(true)
        .from_reader(stdin); 

    let headers = csv_reader.headers().unwrap().clone();

    for record in csv_reader.records() {

        let record = record.unwrap();

        let subj = remap_subject(&record.get(subj_idx).unwrap().to_string(), &expand, &normalise);
        let pred = remap_subject(&record.get(pred_idx).unwrap().to_string(), &expand, &normalise);
        let obj = remap_subject(&record.get(obj_idx).unwrap().to_string(), &expand, &normalise);

        output_nodes.write_all(r#"{"subject":""#.as_bytes()).unwrap();
        output_nodes.write_all(subj.as_bytes()).unwrap();
        output_nodes.write_all(r#"","datasource":""#.as_bytes()).unwrap();
        output_nodes.write_all(datasource_name.as_bytes()).unwrap();
        output_nodes.write_all(r#"","properties":{""#.as_bytes()).unwrap();
        output_nodes.write_all(pred.as_bytes()).unwrap();
            output_nodes.write_all(r#"":[{"#.as_bytes()).unwrap();
            output_nodes.write_all(r#""value":""#.as_bytes()).unwrap();
            output_nodes.write_all(obj.as_bytes()).unwrap();
            output_nodes.write_all(r#"","properties":{"#.as_bytes()).unwrap();
                let mut n = 0;
                let mut is_first = true;
                for column_title in headers.iter() {
                    if !column_title.eq("subject_id") && !column_title.eq("predicate_id") && !column_title.eq("object_id") {
                        let val = record.get(n);
                        if val.is_some() {
                            if is_first {
                                is_first = false;
                            } else {
                                output_nodes.write_all(r#","#.as_bytes()).unwrap();
                            }
                            output_nodes.write_all(r#"""#.as_bytes()).unwrap();
                            output_nodes.write_all(column_title.as_bytes()).unwrap();
                            output_nodes.write_all(r#"":[""#.as_bytes()).unwrap();
                            write_escaped_string(remap_subject(&record.get(n).unwrap().to_string(), &expand, &normalise).as_bytes(), &mut output_nodes);
                            output_nodes.write_all(r#""]"#.as_bytes()).unwrap();
                        }
                    }
                    n = n + 1;
                }
                output_nodes.write_all(r#"}"#.as_bytes()).unwrap(); // sssom
            output_nodes.write_all(r#"}]"#.as_bytes()).unwrap(); // value
        output_nodes.write_all(r#"}"#.as_bytes()).unwrap(); // properties
        output_nodes.write_all(r#"}"#.as_bytes()).unwrap(); // entity
        output_nodes.write_all("\n".as_bytes()).unwrap();
    }


    
}

fn remap_subject(subject: &String, expand:&PrefixMap, normalise:&PrefixMap) -> String {

    let iri = expand.reprefix(subject);
    // eprintln!("{} expanded to {}", subject, iri);
    let curie = normalise.reprefix(&iri);
    // eprintln!("{} compacted to {}", iri, curie);
    return curie;
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

