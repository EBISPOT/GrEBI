use flate2::read::GzDecoder;
use serde_json::value;
use std::cmp::Ordering;
use std::collections::{BTreeSet, VecDeque};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::io::{BufRead, BufReader };
use clap::Parser;
use std::{io};

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
    exclude_props: Option<String>,

    #[arg(long)]
    annotate_subgraph_name: Option<String>,

    #[arg(long)]
    prioritise_datasources: Option<String>,

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

    let exclude_props:BTreeSet<Vec<u8>> = args.exclude_props.as_deref().unwrap_or_default().split(",").map(|s| s.to_string().as_bytes().to_vec()).collect();

    let mut input_filenames: Vec<String> = args._files.to_vec();
    dedup_in_unsorted_vec(&mut input_filenames);

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

    let ds_priorities = args.prioritise_datasources.as_deref().unwrap_or_default().split(",")
        .map(|s| s.to_string().as_bytes().to_vec())
        .collect::<Vec<Vec<u8>>>();

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
                write_merged_entity(&lines_to_write, &mut writer, &inputs, &exclude_props, &ds_priorities, &subgraph_name);
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
        write_merged_entity(&lines_to_write, &mut writer, &inputs, &exclude_props, &ds_priorities, &subgraph_name);
        lines_to_write.clear();
    }

    writer.flush().unwrap();

    Ok(())
}


#[inline(always)]
fn write_merged_entity(lines_to_write: &Vec<BufferedLine>, stdout: &mut BufWriter<std::io::StdoutLock>, inputs: &Vec<Input>, exclude_props:&BTreeSet<Vec<u8>>, ds_priorities: &Vec<Vec<u8>>, subgraph_name:&Option<String>) {

    if lines_to_write.len() == 0 {
        panic!();
    }

    let jsons:Vec<ParsedEntity> = lines_to_write.iter().map(|line| {
        return ParsedEntity::from_json(&line.line, &inputs[line.input_index].datasource );
    }).collect();

    let mut has_any_type:bool = false;

    let mut source_ids: Vec<&[u8]> = Vec::new();
    let mut datasources: Vec<&[u8]> = Vec::new();
    let mut embedding_vectors:Vec<&[u8]> = Vec::new();

    for json in &jsons {
        if json.has_type {
            has_any_type = true;
        }
        for &source_id in json.source_ids.iter() {
            source_ids.push(source_id);
        }
        datasources.push(json.datasource);

        if json.embedding_vector.is_some() {
            embedding_vectors.push(json.embedding_vector.unwrap());
        }
    }

    if !has_any_type {
        // skip if after merging the node has no type
        // this will remove e.g. all the ubergraph entries or sssom mappings
        // where the node is not defined by another datasource 
        return;
    }

    // merge all the {prop_key, prop_value, datasource} into a single list for sorting

    struct MergedProp<'a> {
        datasource:&'a [u8],
        source_ids:&'a Vec<&'a [u8]>,
        key: &'a [u8],
        value: &'a [u8]
    }
    let mut n_props_total = 0;
    for json in &jsons {
        n_props_total += json.props.len();
    }
    let mut merged_props = Vec::<MergedProp>::with_capacity(n_props_total);
    for json in &jsons {
        for prop in json.props.iter() {
            if !exclude_props.contains(prop.key) {
                merged_props.push(MergedProp { datasource: json.datasource, source_ids: &json.source_ids, key: prop.key, value: prop.value });
            }
        }
    }

    if merged_props.len() == 0 {
        // skip if after excluding properties there are none left
        return;
    }

    // we don't sort because the datasources (and therefore the source IDs) are ordered
    // by the order of the input files which indicates the priority of the datasources
    //
    dedup_in_unsorted_vec(&mut datasources);
    dedup_in_unsorted_vec(&mut source_ids);

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

    // source ids here
    stdout.write_all(r#","grebi:sourceIds":["#.as_bytes()).unwrap();
    let mut is_first_sid = true;
    for sid in source_ids {
        if !is_first_sid {
            stdout.write_all(r#","#.as_bytes()).unwrap();
        } else {
            is_first_sid = false;
        }
        stdout.write_all(r#"""#.as_bytes()).unwrap();
        stdout.write_all(sid).unwrap();
        stdout.write_all(r#"""#.as_bytes()).unwrap();
    }
    stdout.write_all(r#"]"#.as_bytes()).unwrap();


    if subgraph_name.is_some() {
        stdout.write_all(r#","grebi:subgraph":""#.as_bytes()).unwrap();
        stdout.write_all(&subgraph_name.as_ref().unwrap().as_bytes()).unwrap();
        stdout.write_all(r#"""#.as_bytes()).unwrap();
    }

    // sort by key, then value, then datasource
    merged_props.sort_by(|a, b| {
        match a.key.cmp(&b.key) {
            Ordering::Equal => {
                match a.value.cmp(&b.value) {
                    Ordering::Equal => {
                        return a.datasource.cmp(&b.datasource);
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
        return a.key == b.key && a.value == b.value && a.datasource == b.datasource;
    });


    // now each key is together, with each value together, then each datasource together
    // and duplicates have been removed. However we want to prioritise the values provided
    // by some datasources. For example we want to take always put labels from MONDO first so
    // the UI can just show the first label and not get a mix of labels from MONDO, EFO, DOID, etc.
    //
    // We do this by sorting the values in the order of the datasources provided on the command line
    // as --prioritise-datasources=OLS.mondo,OLS.efo etc 
    //
    // The other thing we want to do is put the most popular values (greatest number of datasources) first. 
    // This will address things like in the cross-species graph where "diabetes mellitus" is merged
    // with "diabetes mellitus, domestic guinea pig". Both are in MONDO but obviously we want "diabetes mellitus"
    // to be the prioritised label. Because it is also asserted by EFO, DOID, etc it will have
    // the most datasources and be prioritised.
    //
    // So: (a) order of datasource priority provided on command line
    //     (b) number of datasources that define each value
    // 
    let mut groups_by_key_then_value: Vec<Vec<&[MergedProp]>> = {
        let mut groups = Vec::new();
        let mut outer_start = 0;
        while outer_start < merged_props.len() {
            let current_key = merged_props[outer_start].key;
            let mut outer_end = outer_start + 1;
            while outer_end < merged_props.len() && merged_props[outer_end].key == current_key {
                outer_end += 1;
            }
            let key_group = &merged_props[outer_start..outer_end];
            let mut value_groups = Vec::new();
            let mut inner_start = 0;
            while inner_start < key_group.len() {
                let current_value = key_group[inner_start].value;
                let mut inner_end = inner_start + 1;
                while inner_end < key_group.len() && key_group[inner_end].value == current_value {
                    inner_end += 1;
                }
                value_groups.push(&key_group[inner_start..inner_end]);
                inner_start = inner_end;
            }
            groups.push(value_groups);
            outer_start = outer_end;
        }
        groups
    };

    for group_by_value in groups_by_key_then_value.iter_mut() {

        // sort the values for this key

        group_by_value.sort_by(|a, b| {

            let lowest_ds_a = a.iter().map(|prop| prop.datasource).map(|ds| {
                ds_priorities.iter().position(|x| x == ds).unwrap_or(usize::MAX)
            }).min().unwrap_or(usize::MAX);

            let lowest_ds_b = b.iter().map(|prop| prop.datasource).map(|ds| {
                ds_priorities.iter().position(|x| x == ds).unwrap_or(usize::MAX)
            }).min().unwrap_or(usize::MAX);

            match lowest_ds_a.cmp(&lowest_ds_b) {
                Ordering::Equal => {
                    // if the lowest datasource is the same, sort by number of datasources
                    let count_a = a.len();
                    let count_b = b.len();
                    return count_b.cmp(&count_a); // more datasources first
                }
                other => {
                    return other; // lowest datasource first
                }
            }
        });
    }


    let mut index = 0;
    while index < groups_by_key_then_value.len() {

        let key = groups_by_key_then_value.get(index).unwrap()[0][0].key;
        let values:&Vec<&[MergedProp]> = groups_by_key_then_value.get(index).unwrap();

        stdout.write_all(r#",""#.as_bytes()).unwrap();
        stdout.write_all(key).unwrap(); // key from the first entry will do
        stdout.write_all(r#"":["#.as_bytes()).unwrap();

        let mut is_first2 = true;
        for value in values {

            let the_value = value[0].value;
            let datasources = value.iter().map(|prop| prop.datasource);

            let mut source_ids:Vec<&[u8]> = value.iter().flat_map(|prop| prop.source_ids.iter().map(|sid| *sid)).collect();
            source_ids.sort_unstable();

            if !is_first2 {
                stdout.write_all(r#","#.as_bytes()).unwrap();
            } else {
                is_first2 = false;
            }
            stdout.write_all(r#"{"grebi:datasources":["#.as_bytes()).unwrap();

            let mut is_first3 = true;
            for datasource in datasources {
                if !is_first3 {
                    stdout.write_all(r#","#.as_bytes()).unwrap();
                } else {
                    is_first3 = false;
                }
                stdout.write_all(r#"""#.as_bytes()).unwrap();
                stdout.write_all(datasource).unwrap();
                stdout.write_all(r#"""#.as_bytes()).unwrap();
            }

            stdout.write_all(r#"],"grebi:sourceIds":["#.as_bytes()).unwrap();

            let mut last_source_id:Option<&[u8]> = None;
            for index2 in 0..source_ids.len() {
                let source_id = &source_ids[index2];
                if last_source_id.is_some() {
                    if *source_id == last_source_id.unwrap() { // deduplication of source ids
                        continue;
                    }
                    stdout.write_all(b",");
                }
                stdout.write_all(r#"""#.as_bytes()).unwrap();
                stdout.write_all(source_id).unwrap();
                stdout.write_all(r#"""#.as_bytes()).unwrap();
                last_source_id = Some(source_id);
            }

            stdout.write_all(r#"],"grebi:value":"#.as_bytes()).unwrap();
            stdout.write_all(the_value).unwrap();
            stdout.write_all(r#"}"#.as_bytes()).unwrap();

        }

        stdout.write_all(r#"]"#.as_bytes()).unwrap(); // close properties array

        index = index + 1;
    }

    if embedding_vectors.len() > 0 {

        let avg_embedding = average_embeddings(&embedding_vectors);

        stdout.write_all(r#","grebi:embeddingVector":"#.as_bytes()).unwrap();
        stdout.write_all(&avg_embedding).unwrap();
    }

    stdout.write_all(
            r#"}
"#
            .as_bytes(),
        ).unwrap(); // close the line
}

fn average_embeddings(embeddings: &Vec<&[u8]>) -> Vec<u8> {
    if embeddings.is_empty() {
        return vec![];
    }

    let mut parsed_embeddings: Vec<Vec<f32>> = Vec::new();

    for emb in embeddings {
        let s = std::str::from_utf8(emb).expect("Invalid UTF-8");
        let trimmed = s.trim().trim_start_matches('[').trim_end_matches(']');
        let numbers: Vec<f32> = trimmed
            .split(',')
            .map(|x| x.trim().parse::<f32>().expect("Invalid float"))
            .collect();
        parsed_embeddings.push(numbers);
    }

    let len = parsed_embeddings[0].len();
    let mut averages = vec![0.0f32; len];

    for emb in &parsed_embeddings {
        assert_eq!(emb.len(), len, "All embeddings must be the same length");
        for (i, &val) in emb.iter().enumerate() {
            averages[i] += val;
        }
    }

    for avg in &mut averages {
        *avg /= embeddings.len() as f32;
    }

    return format!(
        "[{}]",
        averages
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>()
            .join(", ")
    ).as_bytes().to_vec();
}

fn dedup_in_unsorted_vec<T: PartialEq>(vec: &mut Vec<T>) {
    let mut i = 0;
    while i < vec.len() {
        let mut j = i + 1;
        while j < vec.len() {
            if vec[i] == vec[j] {
                vec.remove(j);
            } else {
                j += 1;
            }
        }
        i += 1;
    }
}
