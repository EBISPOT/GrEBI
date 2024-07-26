
pub mod json_parser;
pub mod json_lexer;
pub mod prefix_map;
pub mod slice_merged_entity;
pub mod slice_materialised_edge;
pub mod load_metadata_mapping_table;

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
            return &json[start-1..end];
        }
        panic!("unexpected char {} in {}", json[end], String::from_utf8(json.to_vec()).unwrap());
    }
}

// get the subjects as an array without parsing the rest of the json
pub fn get_subjects<'a>(json:&'a [u8])->Vec<&'a [u8]> {

    if !json.starts_with("{\"subjects\":[\"".as_bytes()) {
        panic!("could not do quick subject extraction from: {} length {}", String::from_utf8(json.to_vec()).unwrap(), json.len());
    }

    let start = "{\"subjects\":[\"".as_bytes().len();
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
            continue;
        }
        if json[end] == b']' {
            return subjs;
        }
        panic!();
    }
}


pub fn serialize_equivalence(subject:&[u8], object:&[u8]) -> Option<Vec<u8>> {

    if subject.eq(object) {
        return None;
    }

    let mut buf = Vec::with_capacity(subject.len() + object.len() + 2);

    if subject < object {
        buf.extend(subject.iter().map(filter_newlines));
        buf.push(b'\t');
        buf.extend(object.iter().map(filter_newlines));
    } else {
        buf.extend(object.iter().map(filter_newlines));
        buf.push(b'\t');
        buf.extend(subject.iter().map(filter_newlines));
    }

    buf.push(b'\n');
    return Some(buf);
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




