// grebi_reprefix
//
// A small stdio CLI that normalises IRIs/CURIEs to their canonical CURIE form.
// It reads one IRI-or-CURIE per line on stdin and writes the reprefixed CURIE
// (or the input unchanged if no prefix matches) per line on stdout. One input
// line always produces exactly one output line, so callers can pipe a batch and
// read back the same number of lines in order.
//
// The prefix map is supplied as a JSON file (CURIE/URI prefix -> canonical
// prefix); the Java backend fetches it from Postgres and hands us a temp file
// path, mirroring how OLS hands its text_tagger binary a downloaded db file.

use std::collections::HashMap;
use std::env;
use std::io::{self, BufRead, BufReader, BufWriter, Write};

use grebi_shared::prefix_map::PrefixMapBuilder;

fn main() {
    let args = env::args().collect::<Vec<String>>();

    if args.len() != 2 {
        eprintln!("Usage: grebi_reprefix <prefix_map.json>");
        std::process::exit(1);
    }

    let prefix_map = {
        let rdr = BufReader::new(std::fs::File::open(args.get(1).unwrap()).unwrap());
        let mut builder = PrefixMapBuilder::new();
        serde_json::from_reader::<_, HashMap<String, String>>(rdr)
            .unwrap()
            .into_iter()
            .for_each(|(k, v)| {
                builder.add_mapping(k, v);
            });
        builder.build()
    };

    let stdin = io::stdin();
    let mut reader = BufReader::new(stdin.lock());
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());

    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line).unwrap();
        if n == 0 {
            break; // EOF
        }

        // strip the trailing newline (and optional CR) without allocating
        let input = line.trim_end_matches(['\n', '\r']);

        let reprefixed = prefix_map.reprefix(&input.to_string());

        writer.write_all(reprefixed.as_bytes()).unwrap();
        writer.write_all(b"\n").unwrap();
        writer.flush().unwrap();
    }
}
