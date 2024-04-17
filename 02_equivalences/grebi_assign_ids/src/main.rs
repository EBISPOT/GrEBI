


use std::{env, io};
use rocksdb::{DB, Options, BlockBasedOptions};
use std::io::{BufRead, BufReader };
use std::io::{Write, BufWriter};
use grebi_shared::json_lexer::{lex, JsonTokenType};
use grebi_shared::json_parser::JsonParser;

use grebi_shared::find_strings;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() {

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: grebi_assign_ids <rocksdb_path>");
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

    let stdin = io::stdin();
    let handle = stdin.lock();
    let mut reader = BufReader::new(handle);

    let stdout = io::stdout().lock();
    // let mut writer = BufWriter::new(stdout);
    let mut writer = BufWriter::with_capacity(1024*1024*1024 /* 1 GB! */, stdout);

    let mut n_hit = 0;
    let mut n_total = 0;

    loop {
        let mut line: Vec<u8> = Vec::new();
        reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            break;
        }
        
        let mut json = JsonParser::parse(&line);

        let mut id:Option<&[u8]> = None;

        let begin_obj = json.begin_object();
        while json.peek().kind != JsonTokenType::EndObject {
            let prop_key = json.name();

            if prop_key == b"id" {
                id = Some(json.string());
            } else {
                json.value(); // skip
            }
        }
        let end_obj = json.end_object();

        if id.is_none() {
            panic!("Missing id field in JSON: {}", String::from_utf8(line).unwrap());
        }

        let group = db.get_cf(subj_to_group, id.unwrap()).unwrap();

        n_total = n_total + 1;

        if n_total % 1000000 == 0 {
            eprintln!("assigned IDs to {} subjects in {} seconds", n_total, start_time.elapsed().as_secs());
        }

        if group.is_some() {

            // the subject mapped to an equivalence group
            n_hit = n_hit + 1;

            writer.write_all("{\"grebi:nodeId\":\"".as_bytes()).unwrap();
            writer.write_all(group.unwrap().as_slice()).unwrap();
            writer.write_all("\",".as_bytes()).unwrap();
        } else {
            // the subject did not map to an equivalence group
            writer.write_all("{\"grebi:nodeId\":\"".as_bytes()).unwrap();
            writer.write_all(id.unwrap()).unwrap();
            writer.write_all("\",".as_bytes()).unwrap();
        }

        let properties_json = line[begin_obj.index+1..end_obj.index].to_vec();
        let string_locations = find_strings(&properties_json);

        if string_locations.len() == 0 {
            // no strings in the json, just write it as is
            writer.write_all(&properties_json).unwrap();
            writer.write_all("}\n".as_bytes()).unwrap();
            continue;
        }

        // replace any strings in the json that are IDs with their group IDs (if extant)

        let matches = db.batched_multi_get_cf(
            subj_to_group,
            string_locations.iter().map(|(start,end)| &properties_json[*start..*end]).collect::<Vec<&[u8]>>(),
            false
        ).into_iter().collect::<Result<Vec<_>, _>>().unwrap();

        let mut n_ch = 0;
        let mut n_found_str = 0;

        while n_found_str < string_locations.len() {
            let (cur_start, cur_end) = string_locations[n_found_str];
            let str = &properties_json[cur_start..cur_end];

            writer.write_all(&properties_json[n_ch..cur_start]).unwrap();

            let pv_group = matches.get(n_found_str).unwrap();
            if pv_group.is_some() {
                writer.write_all(pv_group.as_deref().unwrap().to_vec().as_slice()).unwrap();
            } else {
                writer.write_all(str).unwrap();
            }

            n_ch = cur_end;
            n_found_str = n_found_str + 1;
        }

        writer.write_all(&properties_json[n_ch..properties_json.len()]).unwrap();
        writer.write_all("}\n".as_bytes()).unwrap();
    }

}

