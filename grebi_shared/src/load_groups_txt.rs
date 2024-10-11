
use std::{collections::HashMap, fs::File, io::{BufRead, BufReader}};

pub fn load_id_to_group_mapping(filename:&str) -> HashMap<Vec<u8>, Vec<u8>> {

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

pub fn load_id_to_group_bidirectional_mapping(filename:&str) -> (HashMap<Vec<u8>, Vec<u8>>, HashMap<Vec<u8>, Vec<Vec<u8>>>) {

    let start_time = std::time::Instant::now();
    let mut reader = BufReader::new(File::open( filename ).unwrap() );
    let mut id_to_group:HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut group_to_ids:HashMap<Vec<u8>, Vec<Vec<u8>>> = HashMap::new();

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
            id_to_group.insert(tokens[i].to_vec(), tokens[0].to_vec());
        }
        group_to_ids.insert(tokens[0].to_vec(), tokens.iter().skip(1).map(|x| x.to_vec()).collect());
    }

    eprintln!("loaded {} id->group bidirectional mappings in {} seconds", id_to_group.len(), start_time.elapsed().as_secs());

    return (id_to_group, group_to_ids);

}
