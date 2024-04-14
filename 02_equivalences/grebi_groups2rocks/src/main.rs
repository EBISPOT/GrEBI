

use std::collections::{HashSet, HashMap};
use std::{env, io};
use rocksdb::{BlockBasedOptions, Options, DB};
use std::io::{BufRead, BufReader };
use std::io::{Write, BufWriter};

fn main() {

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: grebi_groups2rocks <rocksdb_path>");
        std::process::exit(1);
    }

    let start_time = std::time::Instant::now();

    let rocksdb_path = args[1].clone();

    let stdin = io::stdin();
    let handle = stdin.lock();
    let mut reader = BufReader::new(handle);



    let mut block_opts = BlockBasedOptions::default();

    let mut cache = rocksdb::Cache::new_lru_cache(1024*1024*1024*8);
    cache.set_capacity(1024*1024*1024*8);

    block_opts.set_block_cache(&cache);
    block_opts.set_bloom_filter(20.0, true);
    block_opts.set_cache_index_and_filter_blocks(true);

    let mut options = Options::default();
    options.set_block_based_table_factory(&block_opts);
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options.prepare_for_bulk_load();
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);

    let db = DB::open_cf_with_opts(&options, rocksdb_path.clone(), [("subj_to_group",options.clone()),("group_to_subjs",options.clone())].into_iter()).unwrap();

    let subj_to_group = db.cf_handle("subj_to_group").unwrap();
    let group_to_subjs = db.cf_handle("group_to_subjs").unwrap();

    let mut n = 0;

    loop {
        let mut line: Vec<u8> = Vec::new();
        reader.read_until(b'\n', &mut line).unwrap();
        if line.len() == 0 {
            break;
        }
        line.pop(); // remove \n

        let toks = line.split(|c| *c == b'\t').collect::<Vec<&[u8]>>();

        let group_id = toks[0];

        if group_id.len() >= line.len()-1 {
            eprintln!("Error: line contents: {}", String::from_utf8_lossy(line.as_slice()));
        }

        db.put_cf(group_to_subjs, group_id, &line[group_id.len()+1..]).unwrap();

        for n in 1..toks.len() {
            let subj = toks[n];
            db.put_cf(subj_to_group, subj, group_id).unwrap();
        }
        
        n = n + 1;
        if n % 1000000 == 0 {
            eprintln!("...{} groups in {} seconds to {}", n, start_time.elapsed().as_secs(), rocksdb_path);
        }

    }

    eprintln!("Loaded {} groups in {} seconds", n, start_time.elapsed().as_secs());

    let start_time3 = std::time::Instant::now();
    db.compact_range(None::<&[u8]>, None::<&[u8]>);
    eprintln!("Compacting took {} seconds", start_time3.elapsed().as_secs());

}

