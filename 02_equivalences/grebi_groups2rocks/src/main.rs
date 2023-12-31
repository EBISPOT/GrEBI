

use std::collections::{HashSet, HashMap};
use std::{env, io};
use rocksdb::{DB, Options};
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

    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options.prepare_for_bulk_load();
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);

    let db = DB::open_cf_with_opts(&options, rocksdb_path, [("subj_to_group",options.clone()),("group_to_subjs",options.clone())].into_iter()).unwrap();

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

        db.put_cf(group_to_subjs, group_id, &line[group_id.len()+1..]).unwrap();

        for n in 1..toks.len() {
            let subj = toks[n];
            db.put_cf(subj_to_group, subj, group_id).unwrap();
        }
        
        n = n + 1;
        if n % 1000000 == 0 {
            eprintln!("...{} groups in {} seconds", n, start_time.elapsed().as_secs());
        }

    }

    eprintln!("Loaded {} groups in {} seconds", n, start_time.elapsed().as_secs());

    let start_time3 = std::time::Instant::now();
    db.compact_range(None::<&[u8]>, None::<&[u8]>);
    eprintln!("Compacting took {} seconds", start_time3.elapsed().as_secs());

}

