

use crate::json_lexer::{lex, JsonToken, JsonTokenType};
use crate::json_parser::JsonParser;

#[derive(Clone)]
pub struct SlicedProperty<'a> {
    pub kind:JsonTokenType,
    pub key:&'a [u8],
    pub datasources:Vec<&'a [u8]>,
    pub value:&'a [u8]
}

#[derive(Clone)]
pub struct SlicedEntity<'a> {
    pub id:&'a [u8],
    pub props:Vec<SlicedProperty<'a>>
}

impl<'a> SlicedEntity<'a> {

     pub fn from_json(buf:&'a Vec<u8>) -> SlicedEntity<'a> {

        let lexed = lex(&buf);

        let mut parser = JsonParser::from_lexed(lexed);

        let mut id:Option<&[u8]> = None;
        let mut props:Vec<SlicedProperty> = Vec::new();

        // {
        parser.begin_object();

        // "id": ...
        let k_id = parser.name(&buf);
        if k_id != "id".as_bytes() { panic!("expected id as key, got {}", String::from_utf8( k_id.to_vec() ).unwrap()); }
        id = Some(parser.string(&buf));

        while parser.peek().kind != JsonTokenType::EndObject {

            let prop_key = parser.name(&buf);

            parser.begin_array();

                while parser.peek().kind != JsonTokenType::EndArray {

                    parser.begin_object();

                        let mut value_datasources:Vec<&[u8]> = Vec::new();

                        // "grebi:datasources": ...
                        let k_value_datasources = parser.name(&buf);
                        if k_value_datasources != "grebi:datasources".as_bytes() { panic!(); }

                        parser.begin_array();
                            while parser.peek().kind != JsonTokenType::EndArray {
                                value_datasources.push(parser.string(&buf));
                            }
                        parser.end_array();

                        // "grebi:value": ...
                        let k_value_value = parser.name(&buf);
                        if k_value_value != "grebi:value".as_bytes() { panic!(); }

                        let prop_value_kind = parser.peek().kind;
                        let prop_value = parser.value(&buf);

                        props.push(SlicedProperty { kind: prop_value_kind, key: prop_key, datasources: value_datasources, value: prop_value });

                    parser.end_object();
                }

            parser.end_array();
        }
        parser.end_object();



        return SlicedEntity { id: id.unwrap(), props };

    }


}

#[derive(Clone)]
pub struct SlicedReified<'a> {
    pub props:Vec<SlicedProperty<'a>>,
    pub value:&'a [u8],
    pub value_kind: JsonTokenType,
}

impl<'a> SlicedReified<'a> {

     pub fn from_json(buf:&'a Vec<u8>) -> Option<SlicedReified<'a>> {

        let lexed = lex(&buf);
        let mut parser = JsonParser::from_lexed(lexed);

        let mut props:Vec<SlicedProperty> = Vec::new();

        // {
        parser.begin_object();

            // "grebi:value": ...
            let k_value = parser.name(&buf);
            if k_value != "grebi:value".as_bytes() { return None; }

            let value_kind = parser.peek().kind;
            let value = parser.value(&buf);

            // "grebi:properties": ...
            let k_properties = parser.name(&buf);
            if k_properties != "grebi:properties".as_bytes() { return None; }

            parser.begin_object();
            while parser.peek().kind != JsonTokenType::EndObject {

                let prop_key = parser.name(&buf);

                parser.begin_array();

                    while parser.peek().kind != JsonTokenType::EndArray {

                        let kind = parser.peek().kind;
                        let prop_value = parser.value(&buf);

                        props.push(SlicedProperty { kind, key: prop_key, value: prop_value, datasources: Vec::new() });

                    }

                parser.end_array();
            }
            parser.end_object();

        // }
        parser.end_object();

        return Some(SlicedReified { props, value, value_kind });
     }
}
