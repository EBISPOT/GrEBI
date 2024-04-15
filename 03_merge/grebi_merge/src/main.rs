use flate2::read::GzDecoder;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{Write, BufWriter};
use std::io::{BufRead, BufReader };
use std::{env, io};

use grebi_shared::get_id;

mod slice_entity;
use crate::slice_entity::SlicedEntity;
use crate::slice_entity::SlicedProperty;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();

    let stdout = io::stdout().lock();
    let mut writer = BufWriter::with_capacity(1024*1024*32,stdout);

    let mut input_filenames: Vec<String> = args[1..].to_vec();
    input_filenames.sort();
    input_filenames.dedup();

    let mut inputs: Vec<(String, BufReader<GzDecoder<File>>)> = input_filenames
        .iter()
        .map(|file| {
            return (
                file.to_string(),
                BufReader::with_capacity(1024*1024*32,GzDecoder::new(File::open(file).unwrap())),
            );
        })
        .collect();

    if inputs.len() == 0 {
        panic!("No input files");
    }

    let mut lines_to_write: Vec<Vec<u8>> = Vec::new();
    let mut cur_id: Vec<u8> = Vec::new();

    // Get the first line from each file
    let mut cur_lines: VecDeque<(usize /* input index */, Vec<u8>)> = VecDeque::new();

    let mut n = 0;
    loop {
        if n == inputs.len() {
            break;
        }
        let mut line: Vec<u8> = Vec::new();
        inputs[n].1.read_until(b'\n', &mut line).unwrap();
        if line.len() == 0 {
            eprintln!("File appears empty so will not be read: {}", inputs[n].0);
            inputs.remove(n);
            continue;
        }
        cur_lines.push_back((n, line));
        n = n + 1;
    }

    if cur_lines.len() == 0 {
        panic!("Nothing to read from any input file");
    }

    cur_lines.make_contiguous()
        .sort_by(|a, b| {
            return get_id(&a.1).cmp(&get_id(&b.1)); });

    loop {

        // Get the ID from the lowest sorted line
        let id = get_id( &cur_lines[0].1 );

        if !id.eq(&cur_id) {
            // this is a new subject; we have finished the old one (if present)
            if cur_id.len() > 0 {
                write_merged_entity(&lines_to_write, &mut writer);
                lines_to_write.clear();
            }
            cur_id = id.to_vec();
        }

        let line = cur_lines.pop_front().unwrap();
        lines_to_write.push(line.1); // TODO: is this a copy? bc line.1 will never be used again


        // The file that provided the current lowest line is now gone from cur_lines
        // So read the next line from it and insert it into the correct sorted place in cur_lines

        let mut line_buf: Vec<u8> = Vec::new();
        inputs[line.0].1.read_until(b'\n', &mut line_buf).unwrap();

        if line_buf.len() == 0 {
            eprintln!("Finished reading {}", inputs[line.0].0);
            if cur_lines.len() == 0 {
                break;
            }
        } else {
            let new_id = get_id(&line_buf);

            match cur_lines.binary_search_by(|probe| { return get_id(&probe.1).cmp(&new_id); }) {
                Ok(pos) => cur_lines.insert(pos, (line.0, line_buf)),
                Err(pos) => cur_lines.insert(pos, (line.0, line_buf)),
            }
        }
    }

    if cur_id.len() > 0 {
        write_merged_entity(&lines_to_write, &mut writer);
        lines_to_write.clear();
    }

    Ok(())
}


#[inline(always)]
fn write_merged_entity(lines_to_write: &Vec<Vec<u8>>, stdout: &mut BufWriter<std::io::StdoutLock>) {
    if lines_to_write.len() == 0 {
        panic!();
    }

    let jsons:Vec<SlicedEntity> = lines_to_write.iter().map(|line| {
        return SlicedEntity::from_json(line);
    }).collect();

    let mut has_any_type:bool = false;

    let mut datasources: Vec<&[u8]> = jsons
        .iter()
        .map(|json| {
            for prop in &json.props {
                if prop.key == b"grebi:type" {
                    has_any_type = true;
                }
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

    datasources.sort();
    datasources.dedup();

    stdout.write_all(r#"{"id":""#.as_bytes()).unwrap();
    stdout.write_all(jsons[0].id).unwrap();
    stdout.write_all(r#"","#.as_bytes()).unwrap();
    stdout.write_all(r#""subjects":"#.as_bytes()).unwrap();
    stdout.write_all(jsons[0].subjects_block).unwrap();
    stdout.write_all(r#","datasources":["#.as_bytes()).unwrap();
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
    stdout.write_all(r#"],"properties":{"#.as_bytes()).unwrap();



    // merge all the {prop_key, prop_value, datasource} into a single list for sorting
    let mut n_props_total = 0;
    for json in &jsons {
        n_props_total += json.props.len();
    }
    let mut merged_props = Vec::<(&[u8] /* datasource */, SlicedProperty)>::with_capacity(n_props_total);
    for json in &jsons {
        for prop in json.props.iter() {
            merged_props.push(( json.datasource, prop.clone()));
        }
    }

    // sort by key, then value, then datasource
    merged_props.sort_by(|a, b| {
        match a.1.key.cmp(&b.1.key) {
            Ordering::Equal => {
                match a.1.value.cmp(&b.1.value) {
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
        return a.1.key == b.1.key && a.1.value == b.1.value && a.0 == b.0;
    });

    let mut is_first = true;

    let mut index = 0;

    // for each of all the properties (key) that apply to this entity
    while index < merged_props.len() {
        if !is_first {
            stdout.write_all(r#","#.as_bytes()).unwrap();
        } else {
            is_first = false;
        }
        stdout.write_all(r#"""#.as_bytes()).unwrap();
        stdout.write_all(merged_props[index].1.key).unwrap();
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
            stdout.write_all(r#"{"datasources":["#.as_bytes()).unwrap();

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
                
                // if we hit the end of all the property definitions are are done
                if index == merged_props.len() {
                    break;
                }   

                // when we hit another key or value we are done; all the same key/value with different
                // datasources should be right after us.
                if merged_props[index].1.key != merged_props[start_value_index].1.key 
                    ||merged_props[index].1.value != merged_props[start_value_index].1.value 
                {
                    break;
                }
            }

            // now write the value itself (from start_value_index; index should already be at the next value)
            stdout.write_all(r#"],"value":"#.as_bytes()).unwrap();
            stdout.write_all(merged_props[start_value_index].1.value).unwrap();
            stdout.write_all(r#"}"#.as_bytes()).unwrap();

            // if we hit the end of all the property definitions are are done
            if index == merged_props.len() {
                break;
            }   

            // if we changed key, we are done here (onto the next property)
            if merged_props[index].1.key != merged_props[start_value_index].1.key {
                break;
            }
        }
        stdout.write_all(r#"]"#.as_bytes()).unwrap(); // close properties array
    }


    stdout.write_all(
            r#"}}
"#
            .as_bytes(),
        ).unwrap(); // close the lline
}


