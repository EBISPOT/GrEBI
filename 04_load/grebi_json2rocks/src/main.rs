
use flate2::read::GzDecoder;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::io::BufRead;
use std::io::Write;
use grebi_shared::get_subjects;
use clap::Parser;
use rocksdb::DB;
use rocksdb::Options;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(short, long)]
    input_merged_gz_filename: String,

    #[arg(short, long)]
    rocksdb_path: String
}

fn main() {

    let args = Args::parse();

    let start_time = std::time::Instant::now();

    let mut reader = BufReader::new(GzDecoder::new(File::open(args.input_merged_gz_filename).unwrap()));

    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options.prepare_for_bulk_load();
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);

     let db = DB::open(&options, args.rocksdb_path).unwrap();

    let mut line:Vec<u8> = Vec::new();
    let mut n:i64 = 0;

    loop {

        line.clear();
        reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            println!("saw {} subjects", n);
            break;
        }

        let subjs = get_subjects(&line);

        // TODO idk if this is what were gonna do or whatever
        // Is the only reason we're putting this in rocks so we can check for existence of
        // entities??
        // if so we just need a hashset, not a full store
        //
        for subj in subjs {
            db.put(&subj, &line).unwrap();
        }

        n = n + 1;

        if n % 1000000 == 0 {
            println!("{}", n);
        }
    }

    eprintln!("Building took {} seconds", start_time.elapsed().as_secs());

    let start_time2 = std::time::Instant::now();

    db.compact_range(None::<&[u8]>, None::<&[u8]>);


    eprintln!("Compacting took {} seconds", start_time2.elapsed().as_secs());



}