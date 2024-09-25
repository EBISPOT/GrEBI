use flate2::read::GzDecoder;
use std::cmp::Ordering;
use std::collections::{BTreeSet, VecDeque};
use std::fs::File;
use std::io::{Write, BufWriter};
use std::io::{BufRead, BufReader };
use clap::Parser;
use std::{env, io};

use grebi_shared::get_id;

mod parse_entity;
use crate::parse_entity::ParsedEntity;
use crate::parse_entity::ParsedProperty;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

struct Input {
    datasource:Vec<u8>,
    filename:String,
    reader:BufReader<GzDecoder<File>>
}

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    exclude_props: String,

    #[arg(long)]
    annotate_subgraph_name: Option<String>,

     #[arg(trailing_var_arg = true, allow_hyphen_values = true, required = true)]
    _files: Vec<String>,
}

#[derive(Debug)]
struct BufferedLine {
    input_index:usize,
    line:Vec<u8>
}

fn main() -> std::io::Result<()> {

    let args = Args::parse();

    let stdout = io::stdout().lock();
    let mut writer = BufWriter::with_capacity(1024*1024*32,stdout);

    let exclude_props:BTreeSet<Vec<u8>> = args.exclude_props.split(",").map(|s| s.to_string().as_bytes().to_vec()).collect();

    let mut input_filenames: Vec<String> = args._files.to_vec();
    input_filenames.sort();
    input_filenames.dedup();

    let subgraph_name:Option<String> = args.annotate_subgraph_name;

    let mut inputs: Vec<Input> = input_filenames
        .iter()
        .map(|file| {
            let tokens = file.split(':').collect::<Vec<&str>>();
            if tokens.len() != 2 {
                panic!("Inputs file must be of the form <datasource>:<filename>");
            }
            let datasource = tokens[0].to_string();
            let filename = tokens[1].to_string();
            return Input {
                datasource: datasource.as_bytes().to_vec(),
                filename: filename.clone(),
                reader: BufReader::with_capacity(1024*1024*32,GzDecoder::new(File::open(filename).unwrap())),
            };
        })
        .collect();

    if inputs.len() == 0 {
        panic!("No input files");
    }

    let mut cur_id: Vec<u8> = Vec::new();

    // Get the first line from each file
    let mut cur_lines: VecDeque<BufferedLine> = VecDeque::new();
    let mut lines_to_write: Vec<BufferedLine> = Vec::new();

    let mut n = 0;
    loop {
        if n == inputs.len() {
            break;
        }
        let mut line: Vec<u8> = Vec::new();
        inputs[n].reader.read_until(b'\n', &mut line).unwrap();
        if line.len() == 0 {
            eprintln!("File appears empty so will not be read: {}", inputs[n].filename);
            inputs.remove(n);
            continue;
        }
        cur_lines.push_back(BufferedLine { input_index: n, line });
        n = n + 1;
    }

    if cur_lines.len() == 0 {
        panic!("Nothing to read from any input file");
    }

    cur_lines.make_contiguous()
        .sort_by(|a, b| {
            return a.line.cmp(&b.line); });

    //eprintln!("cur_lines: {:?}", cur_lines.iter().map(|line| String::from_utf8(line.line.clone()).unwrap()).collect::<Vec<_>>() );
    //eprintln!("cur_lines values: {:?}", cur_lines);

    loop {

        // Get the ID from the lowest sorted line
        let id = get_id( &cur_lines[0].line );

        if !id.eq(&cur_id) {
            // this is a new subject; we have finished the old one (if present)
            if cur_id.len() > 0 {
                write_merged_entity(&lines_to_write, &mut writer, &inputs, &exclude_props, &subgraph_name);
                lines_to_write.clear();
            }
            cur_id = id.to_vec();
        }

        let line = cur_lines.pop_front().unwrap();
        let input_index = line.input_index;
        lines_to_write.push(line);


        // The file that provided the current lowest line is now gone from cur_lines
        // So read the next line from it and insert it into the correct sorted place in cur_lines

        let mut line_buf: Vec<u8> = Vec::new();
        inputs[input_index].reader.read_until(b'\n', &mut line_buf).unwrap();

        if line_buf.len() == 0 {
            eprintln!("Finished reading {}", inputs[input_index].filename);
            if cur_lines.len() == 0 {
                break;
            }
        } else {
            match cur_lines.binary_search_by(|probe| { return probe.line.cmp(&line_buf); }) {
                Ok(pos) => cur_lines.insert(pos, BufferedLine { input_index, line: line_buf }),
                Err(pos) => cur_lines.insert(pos, BufferedLine { input_index, line: line_buf })
            }
        }
    }

    if cur_id.len() > 0 {
        write_merged_entity(&lines_to_write, &mut writer, &inputs, &exclude_props, &subgraph_name);
        lines_to_write.clear();
    }

    writer.flush().unwrap();

    Ok(())
}

#[inline(always)]
fn write_merged_entity(lines_to_write: &Vec<BufferedLine>, stdout: &mut BufWriter<std::io::StdoutLock>, inputs: &Vec<Input>, exclude_props:&BTreeSet<Vec<u8>>, subgraph_name:&Option<String>) {

    if lines_to_write.len() == 0 {
        panic!();
    }

    let jsons:Vec<ParsedEntity> = lines_to_write.iter().map(|line| {
        return ParsedEntity::from_json(&line.line, &inputs[line.input_index].datasource );
    }).collect();

    let mut has_any_type:bool = false;

    let mut datasources: Vec<&[u8]> = jsons
        .iter()
        .map(|json| {
            if json.has_type {
                has_any_type = true;
            }
            return json.datasource;
        })
        .collect();

    if !has_any_type {
        // skip if after merging the node has no type
        // this will remove e.g. all the ubergraph entries or sssom mappings
        // where the node is not defined by another datasource 
        return;
    }

    // merge all the {prop_key, prop_value, datasource} into a single list for sorting
    let mut n_props_total = 0;
    for json in &jsons {
        n_props_total += json.props.len();
    }
    let mut merged_props = Vec::<(&[u8] /* datasource */, &Vec<&[u8]> /* source ids */, ParsedProperty)>::with_capacity(n_props_total);
    for json in &jsons {
        for prop in json.props.iter() {
            if !exclude_props.contains(prop.key) {
                merged_props.push(( json.datasource, &json.source_ids, prop.clone()));
            }
        }
    }

    if merged_props.len() == 0 {
        // skip if after excluding properties there are none left
        return;
    }

    datasources.sort();
    datasources.dedup();

    stdout.write_all(r#"{"grebi:nodeId":""#.as_bytes()).unwrap();
    stdout.write_all(jsons[0].id).unwrap();
    stdout.write_all(r#"","grebi:datasources":["#.as_bytes()).unwrap();
    let mut is_first = true;
    for datasource in datasources {
        if !is_first {
            stdout.write_all(r#","#.as_bytes()).unwrap();
        } else {
            is_first = false;
        }
        stdout.write_all(r#"""#.as_bytes()).unwrap();
        stdout.write_all(datasource).unwrap();
        stdout.write_all(r#"""#.as_bytes()).unwrap();
    }
    stdout.write_all(r#"]"#.as_bytes()).unwrap();

    if subgraph_name.is_some() {
        stdout.write_all(r#","grebi:subgraph":""#.as_bytes()).unwrap();
        stdout.write_all(&subgraph_name.as_ref().unwrap().as_bytes());
        stdout.write_all(r#"""#.as_bytes()).unwrap();
    }

    // sort by key, then value, then datasource
    merged_props.sort_by(|a, b| {
        match a.2.key.cmp(&b.2.key) {
            Ordering::Equal => {
                match a.2.value.cmp(&b.2.value) {
                    Ordering::Equal => {
                        return a.0.cmp(&b.0);
                    }
                    other => {
                        return other;
                    }
                }
            }
            other => {
                return other;
            }
        }
    });

    // we can get duplicate (datasource,key,value) if the datasource
    // has multiple files that define the same thing (e.g. multiple ontologies that import
    // the same ontology when we import the whole lot as an "Ontologies" datasource)
    merged_props.dedup_by(|a, b| {
        return a.2.key == b.2.key && a.2.value == b.2.value && a.0 == b.0;
    });

    let mut index = 0;

    // for each of all the properties (key) that apply to this entity
    while index < merged_props.len() {
        stdout.write_all(r#",""#.as_bytes()).unwrap();
        stdout.write_all(merged_props[index].2.key).unwrap();
        stdout.write_all(r#"":["#.as_bytes()).unwrap();

        let mut is_first2 = true;

        // for each value, print (a) the datasources which define it and (b) the value itself
        while index < merged_props.len() {
            if !is_first2 {
                stdout.write_all(r#","#.as_bytes()).unwrap();
            } else {
                is_first2 = false;
            }

            let start_value_index = index;
            stdout.write_all(r#"{"grebi:datasources":["#.as_bytes()).unwrap();

            let mut source_ids:Vec<&[u8]> = Vec::new();

            let mut is_first3:bool = true;
            loop {
                if !is_first3 {
                    stdout.write_all(r#","#.as_bytes()).unwrap();
                } else {
                    is_first3 = false;
                }
                // print the datasource
                stdout.write_all(r#"""#.as_bytes()).unwrap();
                stdout.write_all(merged_props[index].0).unwrap();
                stdout.write_all(r#"""#.as_bytes()).unwrap();

                index = index + 1;

                for &source_id in merged_props[index].1.iter() {
                    source_ids.push(&source_id);
                }
                
                // if we hit the end of all the property definitions are are done
                if index == merged_props.len() {
                    break;
                }   

                // when we hit another key or value we are done; all the same key/value with different
                // datasources should be right after us.
                if merged_props[index].2.key != merged_props[start_value_index].2.key 
                    ||merged_props[index].2.value != merged_props[start_value_index].2.value 
                {
                    break;
                }
            }

            source_ids.sort_unstable();
            stdout.write_all(r#"],"grebi:sourceIds":["#.as_bytes()).unwrap();
            let mut last_source_id:Option<&[u8]> = None;
            for index2 in 0..source_ids.len() {
                let source_id = &source_ids[index2];
                if last_source_id.is_some() {
                    if *source_id == last_source_id.unwrap() {
                        continue;
                    }
                    last_source_id = Some(source_id);
                    stdout.write_all(b",");
                }
                stdout.write_all(r#"""#.as_bytes()).unwrap();
                stdout.write_all(source_id).unwrap();
                stdout.write_all(r#"""#.as_bytes()).unwrap();
            }

            // now write the value itself (from start_value_index; index should already be at the next value)
            stdout.write_all(r#"],"grebi:value":"#.as_bytes()).unwrap();
            stdout.write_all(merged_props[start_value_index].2.value).unwrap();
            stdout.write_all(r#"}"#.as_bytes()).unwrap();

            // if we hit the end of all the property definitions are are done
            if index == merged_props.len() {
                break;
            }   

            // if we changed key, we are done here (onto the next property)
            if merged_props[index].2.key != merged_props[start_value_index].2.key {
                break;
            }
        }
        stdout.write_all(r#"]"#.as_bytes()).unwrap(); // close properties array
    }

    stdout.write_all(
            r#"}
"#
            .as_bytes(),
        ).unwrap(); // close the lline
}


