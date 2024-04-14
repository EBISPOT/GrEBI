

use serde_json::{Map, Value};

pub fn remove_empty_fields(val:&Value) -> Option<Value> {

    if val.is_array() {
        let res:Vec<Value> = val.as_array().unwrap().iter().filter_map(|v| remove_empty_fields(v)).collect();
        if res.is_empty() {
            return None;
        }
        return Some(Value::Array(res));
    } else if val.is_object() {
        let res:Map<String,Value> = val.as_object().unwrap().iter().filter_map(|(k, v)| {
                let v2 = remove_empty_fields(v);
                if v2.is_none() {
                    return None;
                } else {
                    return Some((k.clone(), v2.unwrap() ));
                }
            }).collect();
        if res.is_empty() {
            return None;
        }
        return Some(Value::Object(res));
    } else if val.is_string() {
        if val.as_str().unwrap().is_empty() {
            return None;
        }
    } else if val.is_null() {
        return None;
    }
    return Some(val.clone());

}