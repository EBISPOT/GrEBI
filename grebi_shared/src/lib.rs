
pub mod json_lexer;
pub mod json_parser;
pub mod prefix_map;
pub mod leveldb;

// get the id without parsing json
pub fn get_id<'a>(json:&'a [u8])->&'a [u8] {

    if !json.starts_with("{\"id\":\"".as_bytes()) {
        panic!("could not do quick id extraction from: {} length {}", String::from_utf8(json.to_vec()).unwrap(), json.len());
    }

    let start = "{\"id\":\"".as_bytes().len();
    let mut end = start;

    while json[end] != b'"' {
        if json[end] == b'\\' {
            todo!();
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
        buf.extend_from_slice(subject);
        buf.push(b'\t');
        buf.extend_from_slice(object);
    } else {
        buf.extend_from_slice(object);
        buf.push(b'\t');
        buf.extend_from_slice(subject);
    }

    buf.push(b'\n');
    return Some(buf);
}
