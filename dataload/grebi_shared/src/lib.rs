
pub mod json_parser;
pub mod json_lexer;
pub mod prefix_map;
pub mod pgcopy;
pub mod slice_merged_entity;
pub mod slice_materialised_edge;
pub mod load_metadata_mapping_table;
pub mod load_groups_txt;

// get the id without parsing json
pub fn get_id<'a>(json:&'a [u8])->&'a [u8] {

    let start:usize = {
        if json.starts_with("{\"grebi:nodeId\":\"".as_bytes()) {
            b"{\"grebi:nodeId\":\"".len()
        } else if json.starts_with("{\"grebi:edgeId\":\"".as_bytes()) {
            b"{\"grebi:edgeId\":\"".len()
        } else {
            panic!("could not do quick id extraction from: {} length {}", String::from_utf8(json.to_vec()).unwrap(), json.len());
        }
    };

    let mut end = start;

    while json[end] != b'"' {
        if json[end] == b'\\' {
            end += 1;
            //todo!("Found escape sequence in ID in {}", String::from_utf8(json.to_vec()).unwrap());
        }
        end += 1;
    }

    return &json[start..end];
}


// get the subject without parsing json
pub fn get_subject<'a>(json:&'a [u8])->&'a [u8] {

    if !json.starts_with("{\"subject\":\"".as_bytes()) {
        panic!("could not do quick subject extraction from: {} length {}", String::from_utf8(json.to_vec()).unwrap(), json.len());
    }

    let start = "{\"subject\":\"".as_bytes().len();
    let mut end = start;

    while json[end] != b'"' {
        if json[end] == b'\\' {
            todo!();
        }
        end += 1;
    }

    return &json[start..end];
}


// get the subjects as an unparsed block without parsing the rest of the json
pub fn get_subjects_block<'a>(json:&'a [u8])->&'a [u8] {

    if !json.starts_with("{\"subjects\":[\"".as_bytes()) {
        panic!("could not do quick subject extraction from: {} length {}", String::from_utf8(json.to_vec()).unwrap(), json.len());
    }

    let start = "{\"subjects\":[\"".as_bytes().len();
    let mut end = start;

    loop {
        while json[end] != b'"' {
            if json[end] == b'\\' {
                todo!();
            }
            end += 1;
        }

        end = end + 1;

        if json[end] == b',' {
            end = end + 1;
            if json[end] != b'"' {
                panic!();
            }
            end = end + 1;
            continue;
        }
        if json[end] == b']' {
            return &json[start-2..end+1]; // the whole array, brackets included
        }
        panic!("unexpected char {} in {}", json[end], String::from_utf8(json.to_vec()).unwrap());
    }
}

// get the subjects as an array without parsing the rest of the json
pub fn get_subjects<'a>(json:&'a [u8])->Vec<&'a [u8]> {

    if !json.starts_with("{\"subjects\":[\"".as_bytes()) {
        panic!("could not do quick subject extraction from: {} length {}", String::from_utf8(json.to_vec()).unwrap(), json.len());
    }

    let mut start = "{\"subjects\":[\"".as_bytes().len();
    let mut end = start;

    let mut subjs:Vec<&'a [u8]> = Vec::new();

    loop {
        while json[end] != b'"' {
            if json[end] == b'\\' {
                todo!();
            }
            end += 1;
        }

        let subj_slice = &json[start..end];
        subjs.push(subj_slice);

        end = end + 1;

        if json[end] == b',' {
            end = end + 1;
            if json[end] != b'"' {
                panic!();
            }
            end = end + 1;
            // the next subject starts after its opening quote (it used to start at the
            // first subject, so every subject after the first came back with the ones before it)
            start = end;
            continue;
        }
        if json[end] == b']' {
            return subjs;
        }
        panic!();
    }
}


fn filter_newlines(ch:&u8)->u8 {
    if *ch == b'\n' || *ch == b'\t' {
        return b' ';
    } else {
        return *ch;
    }
}

// returns vec of (start,end) tuples
pub fn find_strings<'a>(json:&'a [u8])->Vec<(usize, usize)> {

    let mut strings:Vec<(usize, usize)> = Vec::new();

    let mut i = 0;

    while i < json.len() {
        if json[i] == b'"' {
            i = i + 1;
            let start = i;
            loop {
                if i == json.len() {
                    break;
                }
                if json[i] == b'\\' {
                    i = i + 1;
                    if json[i] == b'u' {
                        i = i + 4;
                    } else {
                        i = i + 1;
                    }
                    continue;
                } else if json[i] == b'"' {
                    strings.push((start, i));
                    break;
                } else {
                    i = i + 1;
                }
            }
        }
        i = i + 1;
    }

    return strings;


}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_id_reads_node_and_edge_ids_without_parsing() {
        assert_eq!(get_id(br#"{"grebi:nodeId":"mondo:0005083","grebi:datasources":["x"]}"#), b"mondo:0005083");
        assert_eq!(get_id(br#"{"grebi:edgeId":"e1","grebi:type":"t"}"#), b"e1");
        // an escaped quote inside the id is skipped over, escape sequence and all
        assert_eq!(get_id(br#"{"grebi:nodeId":"a\"b","x":1}"#), br#"a\"b"#);
    }

    #[test]
    #[should_panic(expected = "could not do quick id extraction")]
    fn get_id_needs_the_id_first() {
        get_id(br#"{"grebi:datasources":[],"grebi:nodeId":"x"}"#);
    }

    #[test]
    fn get_subject_and_get_subjects() {
        assert_eq!(get_subject(br#"{"subject":"efo:1","predicate":"p"}"#), b"efo:1");
        assert_eq!(get_subjects(br#"{"subjects":["a","b:2","c"],"x":1}"#), vec![b"a" as &[u8], b"b:2", b"c"]);
        assert_eq!(get_subjects(br#"{"subjects":["only"]}"#), vec![b"only" as &[u8]]);
        assert_eq!(get_subjects_block(br#"{"subjects":["a","b"],"x":1}"#), br#"["a","b"]"#);
    }

    #[test]
    #[should_panic(expected = "could not do quick subject extraction")]
    fn get_subjects_needs_the_subjects_first() {
        get_subjects(br#"{"x":1,"subjects":["a"]}"#);
    }

    #[test]
    fn find_strings_returns_the_bounds_of_every_string() {
        let json = br#"{"a":"b","c":["d\"e",1,"\u00e9f"]}"#;
        let found: Vec<&[u8]> = find_strings(json).into_iter().map(|(s, e)| &json[s..e]).collect();
        assert_eq!(found, vec![b"a" as &[u8], b"b", b"c", br#"d\"e"#, br#"\u00e9f"#]);
        assert!(find_strings(b"[1,2,null]").is_empty());
        assert!(find_strings(b"").is_empty());
    }

    #[test]
    fn newlines_and_tabs_become_spaces() {
        assert_eq!(filter_newlines(&b'\n'), b' ');
        assert_eq!(filter_newlines(&b'\t'), b' ');
        assert_eq!(filter_newlines(&b'x'), b'x');
    }
}
