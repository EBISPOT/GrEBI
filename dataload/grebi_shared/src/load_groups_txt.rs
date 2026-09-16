
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


#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn temp_file(name: &str, content: &str) -> String {
        let path = std::env::temp_dir().join(format!("grebi_shared_{}_{}", std::process::id(), name));
        File::create(&path).unwrap().write_all(content.as_bytes()).unwrap();
        path.to_string_lossy().to_string()
    }

    #[test]
    fn maps_every_member_id_to_its_group() {
        let path = temp_file("groups.txt", "g1\ta\tb\ng2\tc\ng3\n");
        let mapping = load_id_to_group_mapping(&path);
        assert_eq!(mapping.get(b"a" as &[u8]), Some(&b"g1".to_vec()));
        assert_eq!(mapping.get(b"b" as &[u8]), Some(&b"g1".to_vec()));
        assert_eq!(mapping.get(b"c" as &[u8]), Some(&b"g2".to_vec()));
        assert_eq!(mapping.len(), 3, "a group with no members maps nothing");
        assert_eq!(mapping.get(b"g1" as &[u8]), None, "the group id itself is not a member");
    }

    #[test]
    fn the_bidirectional_mapping_also_lists_each_groups_members() {
        let path = temp_file("groups_bidi.txt", "g1\ta\tb\ng2\tc");
        let (id_to_group, group_to_ids) = load_id_to_group_bidirectional_mapping(&path);
        assert_eq!(id_to_group.get(b"c" as &[u8]), Some(&b"g2".to_vec()));
        assert_eq!(group_to_ids.get(b"g1" as &[u8]), Some(&vec![b"a".to_vec(), b"b".to_vec()]));
        assert_eq!(group_to_ids.get(b"g2" as &[u8]), Some(&vec![b"c".to_vec()]), "a missing final newline does not matter");
    }

    #[test]
    fn an_empty_file_loads_nothing() {
        let path = temp_file("groups_empty.txt", "");
        assert!(load_id_to_group_mapping(&path).is_empty());
    }
}
