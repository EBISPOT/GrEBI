

use crate::json_lexer::{lex, JsonToken, JsonTokenType};
use crate::json_parser::JsonParser;

#[derive(Clone)]
pub struct SlicedPropertyValue<'a> {
    pub kind:JsonTokenType,
    pub datasources:Vec<&'a [u8]>,
    pub value:&'a [u8]
}

#[derive(Clone)]
pub struct SlicedProperty<'a> {
    pub key:&'a [u8],
    pub values_slice:&'a [u8],
    pub values:Vec<SlicedPropertyValue<'a>>
}

#[derive(Clone)]
pub struct SlicedEntity<'a> {
    pub id:&'a [u8],
    pub datasources:Vec<&'a [u8]>,
    pub props:Vec<SlicedProperty<'a>>
}

impl<'a> SlicedEntity<'a> {

     pub fn from_json(buf:&'a Vec<u8>) -> SlicedEntity<'a> {

        let mut parser = JsonParser::parse(&buf);

        let mut props:Vec<SlicedProperty> = Vec::new();
        let mut entity_datasources:Vec<&[u8]> = Vec::new();
        
        // {
        parser.begin_object();

        // "grebi:nodeId": ...
        let k_id = parser.name();
        if k_id != "grebi:nodeId".as_bytes() { panic!("expected grebi:nodeId as key, got {}", String::from_utf8( k_id.to_vec() ).unwrap()); }
        let id = parser.string();

        // "grebi:datasources": ...
        let k_value_datasources = parser.name();
        if k_value_datasources != "grebi:datasources".as_bytes() { panic!(); }
        parser.begin_array();
            while parser.peek().kind != JsonTokenType::EndArray {
                entity_datasources.push(parser.string());
            }
        parser.end_array();

        while parser.peek().kind != JsonTokenType::EndObject {

            let prop_key = parser.name();
            let mut values:Vec<SlicedPropertyValue> = Vec::new();

            let values_slice_begin = parser.begin_array();

                while parser.peek().kind != JsonTokenType::EndArray {

                    parser.begin_object();

                        let mut value_datasources:Vec<&[u8]> = Vec::new();

                        // "grebi:datasources": ...
                        let k_value_datasources = parser.name();
                        if k_value_datasources != "grebi:datasources".as_bytes() { panic!(); }
                        parser.begin_array();
                            while parser.peek().kind != JsonTokenType::EndArray {
                                value_datasources.push(parser.string());
                            }
                        parser.end_array();

                        // "grebi:value": ...
                        let k_value_value = parser.name();
                        if k_value_value != "grebi:value".as_bytes() { panic!(); }

                        let prop_value_kind = parser.peek().kind;
                        let prop_value = parser.value();

                        values.push(SlicedPropertyValue { kind: prop_value_kind, datasources: value_datasources, value: prop_value });

                    parser.end_object();
                }

            let values_slice_end = parser.end_array();

            props.push(SlicedProperty { key: prop_key, values, values_slice: &buf[values_slice_begin.index..values_slice_end.index+1] });
        }
        parser.end_object();



        return SlicedEntity { id, datasources: entity_datasources, props };

    }


}

#[derive(Clone)]
pub struct SlicedReified<'a> {
    pub props:Vec<SlicedProperty<'a>>,
    pub value:&'a [u8],
    pub value_kind: JsonTokenType,
}

impl<'a> SlicedReified<'a> {

     pub fn from_json(buf:&'a &[u8]) -> Option<SlicedReified<'a>> {

        let mut parser = JsonParser::parse(&buf);

        let mut props:Vec<SlicedProperty> = Vec::new();

        // {
        parser.begin_object();
            
            if parser.peek().kind == JsonTokenType::EndObject { return None; }

            // "grebi:value": ...
            let k_value = parser.name();
            if k_value != "grebi:value".as_bytes() { return None; }

            let value_kind = parser.peek().kind;
            let value = parser.value();

            // "grebi:properties": ...
            let k_properties = parser.name();
            if k_properties != "grebi:properties".as_bytes() { return None; }

            parser.begin_object();
            while parser.peek().kind != JsonTokenType::EndObject {

                let prop_key = parser.name();
                let mut values:Vec<SlicedPropertyValue> = Vec::new();

                let values_slice_begin = parser.begin_array();

                    while parser.peek().kind != JsonTokenType::EndArray {

                        let kind = parser.peek().kind;
                        let prop_value = parser.value();

                        values.push(SlicedPropertyValue { kind, value: prop_value, datasources: Vec::new() });

                    }

                let values_slice_end = parser.end_array();

                props.push(SlicedProperty { key: prop_key, values, values_slice: &buf[values_slice_begin.index..values_slice_end.index+1]});
            }
            parser.end_object();

        // }
        parser.end_object();

        return Some(SlicedReified { props, value, value_kind });
     }
}
