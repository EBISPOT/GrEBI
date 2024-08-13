
use std::{collections::HashMap, fs::File, io::{BufRead, BufReader}};


pub fn load_groups_txt(filename:&str) -> HashMap<Vec<u8>, Vec<u8>> {

    let start_time = std::time::Instant::now();
    let mut reader = BufReader::new(File::open( filename ).unwrap() );
    let mut mapping:HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

    loop {
        let mut line: Vec<u8> = Vec::new();
        reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            break;
        }
        if line[line.len() - 1] == b'\n' {
            line.pop();
        }

        let tokens:Vec<&[u8]> = line.split(|&x| x == b'\t').collect();

        for i in 1..tokens.len() {
            mapping.insert(tokens[i].to_vec(), tokens[0].to_vec());
        }
    }

    eprintln!("loaded {} id->group mappings in {} seconds", mapping.len(), start_time.elapsed().as_secs());

    return mapping;

}

