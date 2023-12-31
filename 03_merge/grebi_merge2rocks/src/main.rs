use flate2::read::GzDecoder;
use flate2::Compression;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{Write, BufWriter};
use std::io::{BufRead, BufReader, Lines};
use std::{env, io};
use rocksdb::DB;
use rocksdb::Options;
use clap::Parser;

use grebi_shared::get_subjects_block;

mod slice_entity;
use crate::slice_entity::SlicedEntity;
use crate::slice_entity::SlicedProperty;


#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() -> std::io::Result<()> {


    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        eprintln!("Usage: grebi_merge2rocks <rocksdb_path> <input_merged_gz_filename> [<input_merged_gz_filename> ...]");
        std::process::exit(1);
    }

    let start_time = std::time::Instant::now();

    let rocksdb_path = args[1].clone();

    let mut input_filenames: Vec<String> = args[2..].to_vec();
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




    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options.prepare_for_bulk_load();
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);

     let db = DB::open(&options, rocksdb_path).unwrap();




    let mut lines_to_write: Vec<Vec<u8>> = Vec::new();
    let mut cur_subject: Vec<u8> = Vec::new();

    // Get the first line from each file
    let mut cur_lines: VecDeque<(usize /* input index */, Vec<u8>)> = VecDeque::new();

    let mut n = 0;
    loop {
        let mut line: Vec<u8> = Vec::new();
        inputs[n].1.read_until(b'\n', &mut line).unwrap();
        if line.len() == 0 {
            eprintln!("File appears empty so will not be read: {}", inputs[n].0);
            inputs.remove(n);
            continue;
        }
        cur_lines.push_back((n, line));
        n = n + 1;
        if n == inputs.len() {
            break;
        }
    }

    if cur_lines.len() == 0 {
        panic!("nothing to read??");
    }

    cur_lines.make_contiguous()
        .sort_by(|a, b| {
            return get_subjects_block(&a.1).cmp(&get_subjects_block(&b.1)); });


    let mut write_buf = Vec::new();

    loop {

        // Get the lowest sorted line
        let subject = get_subjects_block( &cur_lines[0].1 );

        if !subject.eq(&cur_subject) {
            // this is a new subject; we have finished the old one (if present)
            if cur_subject.len() > 0 {

                write_subject(&lines_to_write, &mut write_buf);
                db.put(subject, &write_buf).unwrap();

                write_buf.clear();
                lines_to_write.clear();
            }
            cur_subject = subject.to_vec();
        }

        let line = cur_lines.pop_front().unwrap();
        lines_to_write.push(line.1); // TODO: is this a copy? bc line.1 will never be used again


        // Get the next line from the file that provided the current lowest

        let mut line_buf: Vec<u8> = Vec::new();
        inputs[line.0].1.read_until(b'\n', &mut line_buf).unwrap();

        if line_buf.len() == 0 {
            eprintln!("Finished reading {}", inputs[line.0].0);
            if cur_lines.len() == 0 {
                break;
            }
        } else {
            let new_subj = get_subjects_block(&line_buf);

            match cur_lines.binary_search_by(|probe| { return get_subjects_block(&probe.1).cmp(&new_subj); }) {
                Ok(pos) => cur_lines.insert(pos, (line.0, line_buf)),
                Err(pos) => cur_lines.insert(pos, (line.0, line_buf)),
            }
        }
    }

    if cur_subject.len() > 0 {
        write_subject(&lines_to_write, &mut write_buf);
        db.put(cur_subject, &write_buf).unwrap();
    }

    eprintln!("Merging took {} seconds", start_time.elapsed().as_secs());



    // TODO the final subject!



    let start_time2 = std::time::Instant::now();
    db.compact_range(None::<&[u8]>, None::<&[u8]>);
    eprintln!("Compacting took {} seconds", start_time2.elapsed().as_secs());

    Ok(())
}


#[inline(always)]
fn write_subject(lines_to_write: &Vec<Vec<u8>>, writer:&mut Vec<u8>) {
    if lines_to_write.len() == 0 {
        panic!();
    }

    let jsons:Vec<SlicedEntity> = lines_to_write.iter().map(|line| {
        return SlicedEntity::from_json(line);
    }).collect();

    let mut datasources: Vec<&[u8]> = jsons
        .iter()
        .map(|json| {
            return json.datasource;
        })
        .collect();

    datasources.sort();
    datasources.dedup();



    writer.write_all(r#"{"subjects":["#.as_bytes()).unwrap();
    writer.write_all(jsons[0].subjects_block).unwrap();
    writer.write_all(r#"],"datasources":["#.as_bytes()).unwrap();
    let mut is_first = true;
    for datasource in datasources {
        if !is_first {
            writer.write_all(r#","#.as_bytes()).unwrap();
        } else {
            is_first = false;
        }
        writer.write_all(r#"""#.as_bytes()).unwrap();
        writer.write_all(datasource).unwrap();
        writer.write_all(r#"""#.as_bytes()).unwrap();
    }
    writer.write_all(r#"],"properties":{"#.as_bytes()).unwrap();



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

    let mut is_first = true;

    let mut index = 0;

    // for each of all the properties (key) that apply to this entity
    while index < merged_props.len() {
        if !is_first {
            writer.write_all(r#","#.as_bytes()).unwrap();
        } else {
            is_first = false;
        }
        writer.write_all(r#"""#.as_bytes());
        writer.write_all(merged_props[index].1.key);
        writer.write_all(r#"":["#.as_bytes());

        let mut is_first2 = true;

        // for each value, print (a) the datasources which define it and (b) the value itself
        while index < merged_props.len() {
            if !is_first2 {
                writer.write_all(r#","#.as_bytes()).unwrap();
            } else {
                is_first2 = false;
            }

            let start_value_index = index;
            writer.write_all(r#"{"datasources":["#.as_bytes()).unwrap();

            let mut is_first3:bool = true;
            loop {
                if !is_first3 {
                    writer.write_all(r#","#.as_bytes()).unwrap();
                } else {
                    is_first3 = false;
                }
                writer.write_all(r#"""#.as_bytes()).unwrap();
                writer.write_all(merged_props[index].0).unwrap();
                writer.write_all(r#"""#.as_bytes()).unwrap();

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
            writer.write_all(r#"],"value":"#.as_bytes()).unwrap();
            writer.write_all(merged_props[start_value_index].1.value).unwrap();
            writer.write_all(r#"}"#.as_bytes()).unwrap();

            // if we hit the end of all the property definitions are are done
            if index == merged_props.len() {
                break;
            }   

            // if we changed key, we are done here (onto the next property)
            if merged_props[index].1.key != merged_props[start_value_index].1.key {
                break;
            }
        }
        writer.write_all(r#"]"#.as_bytes()); // close properties array
    }


    writer.write_all(
            r#"}}
"#
            .as_bytes(),
        ).unwrap(); // close the lline
}



