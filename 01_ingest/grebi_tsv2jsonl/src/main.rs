
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, self, BufReader, StdinLock, StdoutLock, Write, BufRead, Read};
use std::ptr::eq;
use clap::Parser;
use grebi_shared::prefix_map::PrefixMap;
use grebi_shared::prefix_map::PrefixMapBuilder;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    tsv_array_delimiter: Option<String>,

    #[arg(long)]
    tsv_columns: Option<String>,

    #[arg(long, action)]
    tsv_ignore_empty_fields: bool
}

fn main() {

    let args = Args::parse();

    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);

    let stdout = io::stdout().lock();
    let mut output_nodes = BufWriter::new(stdout);




    // skip comments e.g. for CTD
    let mut comment_lines:Vec<String> = Vec::new();
    loop {
        let buf = reader.fill_buf().unwrap();
        if buf.len() == 0 {
            break;
        }
        if buf.starts_with(b"#") {
            let mut header_line = String::new();
            reader.read_line(&mut header_line).unwrap();
            comment_lines.push(header_line);
        } else {
            break;
        }
    } 

    let mut csv_reader =
        csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(args.tsv_columns.is_none())
        .flexible(true)
        .from_reader(reader); 

    let headers = {
        if args.tsv_columns.is_some() {
            args.tsv_columns.unwrap().split(",").map(|s| s.to_string()).collect::<Vec<String>>()
        } else {
            csv_reader.headers().unwrap().iter().map(|s| s.to_string()).collect::<Vec<String>>()
        }
    };

    let arr_delim = {
        if args.tsv_array_delimiter.is_some() {
            Some(args.tsv_array_delimiter.unwrap().as_bytes()[0])
        } else {
            None
        }
    };

    for record in csv_reader.records() {

        let record = record.unwrap();

        output_nodes.write_all(r#"{"#.as_bytes()).unwrap();
            let mut is_first = true;
            let mut n = 0;
            for column_title in headers.iter() {
                    let val = record.get(n);
                    if val.is_some() {
                        if args.tsv_ignore_empty_fields && val.unwrap().len() == 0 {
                            continue;
                        }
                        if is_first {
                            is_first = false;
                        } else {
                            output_nodes.write_all(r#","#.as_bytes()).unwrap();
                        }
                        output_nodes.write_all(r#"""#.as_bytes()).unwrap();
                        output_nodes.write_all(column_title.as_bytes()).unwrap();
                        if arr_delim.is_some() { 
                            output_nodes.write_all(r#"":["#.as_bytes()).unwrap();
                            let mut arr = val.unwrap().as_bytes().split(|b| *b == arr_delim.unwrap());
                            let mut is_first_elem = true;
                            for item in arr {
                                if args.tsv_ignore_empty_fields && item.len() == 0 {
                                    continue;
                                }
                                if is_first_elem {
                                    is_first_elem = false;
                                } else {
                                    output_nodes.write_all(r#","#.as_bytes()).unwrap();
                                }
                                output_nodes.write_all(r#"""#.as_bytes()).unwrap();
                                write_escaped_string(item, &mut output_nodes);
                                output_nodes.write_all(r#"""#.as_bytes()).unwrap();
                            }
                            output_nodes.write_all(r#"]"#.as_bytes()).unwrap();
                        } else {
                            output_nodes.write_all(r#"":[""#.as_bytes()).unwrap();
                            write_escaped_string(&record.get(n).unwrap().as_bytes(), &mut output_nodes);
                            output_nodes.write_all(r#""]"#.as_bytes()).unwrap();
                        }
                    }
                    n = n + 1;
                    if n >= record.len() {
                        break;
                    }
                }
        output_nodes.write_all(b"}\n").unwrap(); // sssom
    }
}

fn write_escaped_string(str:&[u8], writer:&mut BufWriter<StdoutLock>) {
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

