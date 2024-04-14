


use std::collections::{HashSet, HashMap};
use std::ops::Deref;
use std::{env, io};
use rocksdb::{DB, Options, BlockBasedOptions};
use std::io::{BufRead, BufReader };
use std::io::{Write, BufWriter};

use grebi_shared::{get_subject, find_strings};

mod slice_entity;
use slice_entity::SlicedEntity;
use slice_entity::SlicedProperty;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() {

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: grebi_expand_subjects <rocksdb_path>");
        std::process::exit(1);
    }

    let start_time = std::time::Instant::now();

    let rocksdb_path = args[1].clone();

    let mut options = Options::default();
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);

    let mut block_opts = BlockBasedOptions::default();

    let mut cache = rocksdb::Cache::new_lru_cache(1024*1024*1024*8);
    cache.set_capacity(1024*1024*1024*8);


    block_opts.set_block_cache(&cache);
    block_opts.set_bloom_filter(20.0, true);
    block_opts.set_cache_index_and_filter_blocks(true);
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block_opts.set_pin_top_level_index_and_filter(true);
    options.set_block_based_table_factory(&block_opts);
    options.set_stats_dump_period_sec(30);
    options.set_max_open_files(900); // codon limit is 1024 per process
    options.set_max_file_opening_threads(64);

    // let db = DB::open_cf_with_opts_for_read_only(&options, rocksdb_path, [("subj_to_group",options.clone()),("group_to_subjs",options.clone())].into_iter(), false).unwrap();
    let db = DB::open_cf_with_opts_for_read_only(&options, rocksdb_path, [("subj_to_group",options.clone()),("group_to_subjs",options.clone())].into_iter(), false).unwrap();

    let subj_to_group = db.cf_handle("subj_to_group").unwrap();
    let group_to_subjs = db.cf_handle("group_to_subjs").unwrap();

    let stdin = io::stdin();
    let handle = stdin.lock();
    let mut reader = BufReader::new(handle);

    let stdout = io::stdout().lock();
    // let mut writer = BufWriter::new(stdout);
    let mut writer = BufWriter::with_capacity(1024*1024*1024 /* 1 GB! */, stdout);

    let mut n_hit = 0;
    let mut n_total = 0;
    let mut n_prop_key_hit = 0;
    let mut n_prop_key_total = 0;
    let mut n_prop_val_hit = 0;
    let mut n_prop_val_total = 0;

    loop {
        let mut line: Vec<u8> = Vec::new();
        reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            break;
        }
        
        // eprintln!("line: {}", String::from_utf8(line.clone()).unwrap());

        let sliced = SlicedEntity::from_json(&line);

        let subject:&[u8] = sliced.subject;

        let group = db.get_cf(subj_to_group, subject).unwrap();

        n_total = n_total + 1;

        if n_total % 1000000 == 0 {
            eprintln!("expanded {} subjects in {} seconds", n_total, start_time.elapsed().as_secs());
        }

        if group.is_some() {

            // the subject mapped to an equivalence group
            n_hit = n_hit + 1;

            let all_subjs = db.get_cf(group_to_subjs, group.clone().unwrap()).unwrap().unwrap();

            // eprintln!("group {} mapped to subjects {}", String::from_utf8(group.clone().unwrap().to_vec()).unwrap(), String::from_utf8(all_subjs.to_vec()).unwrap());

            writer.write_all("{\"id\":\"".as_bytes()).unwrap();
            writer.write_all(group.unwrap().as_slice()).unwrap();
            writer.write_all("\",\"subjects\":[".as_bytes()).unwrap();
            let mut is_first = true;
            for subj in all_subjs.split(|c| *c == b'\t') {
                if is_first {
                    is_first = false;
                } else{
                    writer.write_all(b",").unwrap();
                }
                writer.write_all(b"\"").unwrap();
                write_escaped_string(&subj, &mut writer);
                writer.write_all(b"\"").unwrap();
            }
            writer.write_all(r#"],"#.as_bytes()).unwrap();

        } else {

            // the subject did not map to an equivalence group

            writer.write_all("{\"id\":\"".as_bytes()).unwrap();
            write_escaped_string(&subject, &mut writer);
            writer.write_all("\",\"subjects\":[\"".as_bytes()).unwrap();
            write_escaped_string(&subject, &mut writer);
            writer.write_all(r#""],"#.as_bytes()).unwrap();
        }
        writer.write_all(r#""datasource":""#.as_bytes()).unwrap();
        writer.write_all(sliced.datasource).unwrap();
        writer.write_all(r#"","properties":{"#.as_bytes()).unwrap();
        let mut is_first = true;

        for prop in sliced.props {
            if !is_first {
                writer.write_all(r#","#.as_bytes()).unwrap();
            } else {
                is_first = false;
            }

            writer.write_all(r#"""#.as_bytes()).unwrap();

            let key_group = db.get_cf(subj_to_group, prop.key).unwrap();
            n_prop_key_total = n_prop_key_total + 1;
            if key_group.is_some() {
                n_prop_key_hit = n_prop_key_hit + 1;
                writer.write_all(key_group.unwrap().as_slice()).unwrap();
            } else {
                writer.write_all(prop.key).unwrap();
            }

            writer.write_all(r#"":["#.as_bytes()).unwrap();

            let mut is_first2 = true;
            for value in prop.values {
                if !is_first2 {
                    writer.write_all(r#","#.as_bytes()).unwrap();
                } else {
                    is_first2 = false;
                }

                // eprintln!("looking up {}", String::from_utf8(value.to_vec()).unwrap());

                // check if the prop value maps to a group
                if value.starts_with(b"\"") {
                    // fast path for string values
                    n_prop_val_total = n_prop_val_total + 1;
                    let value_inner = &value[1..value.len()-1];
                    let pv_group = db.get_cf(subj_to_group, value_inner).unwrap();
                    if pv_group.is_some() {
                        n_prop_val_hit = n_prop_val_hit + 1;
                        writer.write_all(r#"""#.as_bytes()).unwrap();
                        writer.write_all(pv_group.unwrap().as_slice()).unwrap();
                        writer.write_all(r#"""#.as_bytes()).unwrap();
                    } else {
                        writer.write_all(value).unwrap();
                    }
                } else {

                    // find all strings in the value and look them up

                    let string_locations_in_value = find_strings(&value);

                    if string_locations_in_value.len() == 0 {
                        continue;
                    }

                    let matches = db.batched_multi_get_cf(
                        subj_to_group,
                        string_locations_in_value.iter().map(|(start,end)| &value[*start..*end]).collect::<Vec<&[u8]>>(),
                        false
                    ).into_iter().collect::<Result<Vec<_>, _>>().unwrap();

                    let mut n_ch = 0;
                    let mut n_found_str = 0;

                    while n_found_str < string_locations_in_value.len() {
                        let (cur_start, cur_end) = string_locations_in_value[n_found_str];
                        let str = &value[cur_start..cur_end];

                        writer.write_all(&value[n_ch..cur_start]).unwrap();

                        // let pv_group = db.get_cf(subj_to_group, str).unwrap();
                        let pv_group = matches.get(n_found_str).unwrap();
                        n_prop_val_total = n_prop_val_total + 1;
                        if pv_group.is_some() {
                            n_prop_val_hit = n_prop_val_hit + 1;
                            writer.write_all(pv_group.as_deref().unwrap().to_vec().as_slice()).unwrap();
                        } else {
                            writer.write_all(str).unwrap();
                        }

                        n_ch = cur_end;
                        n_found_str = n_found_str + 1;
                    }

                    writer.write_all(&value[n_ch..value.len()]).unwrap();
                }
            }
            writer.write_all(r#"]"#.as_bytes()).unwrap();

        }

        writer.write_all(r#"}}"#.as_bytes()).unwrap(); // close properties object and the entity object
        writer.write_all("\n".as_bytes()).unwrap();
    }

    eprintln!("Expanded {}/{} subjects, {}/{} prop keys, and {}/{} prop values in {} seconds", n_hit, n_total, n_prop_key_hit, n_prop_key_total, n_prop_val_hit, n_prop_val_total, start_time.elapsed().as_secs());

}



fn write_escaped_string(str:&[u8], writer:&mut BufWriter<io::StdoutLock>) {
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
