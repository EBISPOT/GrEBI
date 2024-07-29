
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, self, BufReader, StdinLock, StdoutLock, Write, BufRead, Read};
use std::ptr::eq;
use clap::Parser;
use grebi_shared::prefix_map::PrefixMap;
use grebi_shared::prefix_map::PrefixMapBuilder;
use serde_json::json;
use serde_yaml;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    datasource_name: String,

    #[arg(long)]
    filename: String,
}

fn main() {

    let args = Args::parse();

    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);

    let datasource_name = args.datasource_name.as_str();

    let stdout = io::stdout().lock();
    let mut output_nodes = BufWriter::new(stdout);

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
        .flexible(true)
        .from_reader(reader); // TODO not supposed to do this on a buffered reader... but I did

    let headers = csv_reader.headers().unwrap().clone();
    let subj_idx = headers.iter().position(|h| h.eq("subject_id")).unwrap();
    let pred_idx = headers.iter().position(|h| h.eq("predicate_id")).unwrap();
    let obj_idx = headers.iter().position(|h| h.eq("object_id")).unwrap();

    for record in csv_reader.records() {

        let record = record.unwrap();

        let subj = remap_subject(&record.get(subj_idx).unwrap().to_string(), &expand);
        let pred = remap_subject(&record.get(pred_idx).unwrap().to_string(), &expand);
        let obj = remap_subject(&record.get(obj_idx).unwrap().to_string(), &expand);

        output_nodes.write_all(r#"{"id":""#.as_bytes()).unwrap();
        output_nodes.write_all(subj.as_bytes()).unwrap();
        output_nodes.write_all(r#"",""#.as_bytes()).unwrap();
        output_nodes.write_all(pred.as_bytes()).unwrap();
            output_nodes.write_all(r#"":[{"#.as_bytes()).unwrap();
            output_nodes.write_all(r#""grebi:value":""#.as_bytes()).unwrap();
            output_nodes.write_all(obj.as_bytes()).unwrap();
            output_nodes.write_all(r#"","grebi:properties":{"#.as_bytes()).unwrap();
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
                            write_escaped_string(remap_subject(&record.get(n).unwrap().to_string(), &expand).as_bytes(), &mut output_nodes);
                            output_nodes.write_all(r#""]"#.as_bytes()).unwrap();
                        }
                    }
                    n = n + 1;
                }
                output_nodes.write_all(r#"}"#.as_bytes()).unwrap(); // reified props
            output_nodes.write_all(r#"}]"#.as_bytes()).unwrap(); // value
        output_nodes.write_all(r#"}"#.as_bytes()).unwrap();
        output_nodes.write_all("\n".as_bytes()).unwrap();
    }


    
}

fn remap_subject(subject: &String, expand:&PrefixMap) -> String {

    return expand.reprefix(subject);
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

