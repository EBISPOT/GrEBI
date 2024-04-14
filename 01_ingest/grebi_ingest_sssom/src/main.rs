
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
        let rdr = BufReader::new( std::fs::File::open("/home/james/grebi2/prefix_map_normalise.json").unwrap() );
        let mut builder = PrefixMapBuilder::new();
        serde_json::from_reader::<_, HashMap<String, String>>(rdr).unwrap().into_iter().for_each(|(k, v)| {
            builder.add_mapping(k, v);
        });
        builder.build()
    };

    let mut yaml_header_lines:Vec<String> = Vec::new();

    loop {
        let buf = reader.fill_buf().unwrap();
        if buf.len() == 0 {
            break;
        }
        if buf.starts_with(b"#") {
            let mut header_line = String::new();
            reader.read_line(&mut header_line).unwrap();
            yaml_header_lines.push(header_line);
        } else {
            break;
        }
    } 

    let yaml = yaml_header_lines.iter().map(|line| {
        return line.trim_start_matches("#").to_string();
    }).collect::<Vec<String>>().join("\n");

    let yaml_header:serde_yaml::Value = serde_yaml::from_str::<serde_yaml::Value>(yaml.as_str()).unwrap();

    let yaml_header_curie_map = yaml_header.get("curie_map").unwrap().as_mapping().unwrap();

    let expand:PrefixMap = {
        let mut builder = PrefixMapBuilder::new();
        for (k, v) in yaml_header_curie_map {
            builder.add_mapping(k.as_str().unwrap().to_string() + ":", v.as_str().unwrap().to_string());
        }
        builder.build()
    };

    let mut csv_reader =
        csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .from_reader(reader); // TODO not supposed to do this on a buffered reader... but I did

    let headers = csv_reader.headers().unwrap().clone();
    let subj_idx = headers.iter().position(|h| h.eq("subject_id")).unwrap();
    let pred_idx = headers.iter().position(|h| h.eq("predicate_id")).unwrap();
    let obj_idx = headers.iter().position(|h| h.eq("object_id")).unwrap();

    for record in csv_reader.records() {

        let record = record.unwrap();

        let subj = remap_subject(&record.get(subj_idx).unwrap().to_string(), &expand, &normalise);
        let pred = remap_subject(&record.get(pred_idx).unwrap().to_string(), &expand, &normalise);
        let obj = remap_subject(&record.get(subj_idx).unwrap().to_string(), &expand, &normalise);

        output_nodes.write_all(r#"{"subject":""#.as_bytes()).unwrap();
        output_nodes.write_all(subj.as_bytes()).unwrap();
        output_nodes.write_all(r#"","datasource":""#.as_bytes()).unwrap();
        output_nodes.write_all(datasource_name.as_bytes()).unwrap();
        output_nodes.write_all(r#"","properties":{""#.as_bytes()).unwrap();
        output_nodes.write_all(pred.as_bytes()).unwrap();
            output_nodes.write_all(r#"":{"#.as_bytes()).unwrap();
            output_nodes.write_all(r#""value":""#.as_bytes()).unwrap();
            output_nodes.write_all(obj.as_bytes()).unwrap();
            output_nodes.write_all(r#"","sssom":{"#.as_bytes()).unwrap();
                let mut n = 0;
                let mut is_first = true;
                for column_title in headers.iter() {
                    if is_first {
                        is_first = false;
                    } else {
                        output_nodes.write_all(r#","#.as_bytes()).unwrap();
                    }
                    output_nodes.write_all(r#"""#.as_bytes()).unwrap();
                    output_nodes.write_all(column_title.as_bytes()).unwrap();
                    output_nodes.write_all(r#"":""#.as_bytes()).unwrap();
                    write_escaped_string(remap_subject(&record.get(n).unwrap().to_string(), &expand, &normalise).as_bytes(), &mut output_nodes);
                    output_nodes.write_all(r#"""#.as_bytes()).unwrap();
                    n = n + 1;
                }
                output_nodes.write_all(r#"}"#.as_bytes()).unwrap(); // sssom
            output_nodes.write_all(r#"}"#.as_bytes()).unwrap(); // value
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


fn write_escaped_string(str:&[u8], writer:&mut BufWriter<File>) {
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

