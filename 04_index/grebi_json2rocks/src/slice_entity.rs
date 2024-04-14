

use grebi_shared::json_lexer::{lex, JsonToken, JsonTokenType};
use grebi_shared::json_parser::JsonParser;

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
    pub subjects:Vec<&'a [u8]>,
    pub datasources:Vec<&'a [u8]>,
    pub props:Vec<SlicedProperty<'a>>
}

impl<'a> SlicedEntity<'a> {

     pub fn from_json(buf:&'a Vec<u8>) -> SlicedEntity<'a> {

        let lexed = lex(&buf);

        let mut parser = JsonParser::from_lexed(lexed);

        let mut id:Option<&[u8]> = None;
        let mut subjects:Vec<&[u8]> = Vec::new();
        let mut datasources:Vec<&[u8]> = Vec::new();
        let mut props:Vec<SlicedProperty> = Vec::new();

        // {
        parser.begin_object();

            // "id": ...
            let k_id = parser.name(&buf);
            if k_id != "id".as_bytes() { panic!("expected id as key, got {}", String::from_utf8( k_id.to_vec() ).unwrap()); }
            id = Some(parser.string(&buf));

            // "subjects": ...
            let k_subjects = parser.name(&buf);
            if k_subjects != "subjects".as_bytes() { panic!(); }
            parser.begin_array();
                while parser.peek().kind != JsonTokenType::EndArray {
                    subjects.push(parser.string(&buf));
                }
            parser.end_array();

            // "datasources": ...
            let k_datasources = parser.name(&buf);
            if k_datasources != "datasources".as_bytes() { panic!(); }
            parser.begin_array();
                while parser.peek().kind != JsonTokenType::EndArray {
                    datasources.push(parser.string(&buf));
                }
            parser.end_array();

            // "properties": ...
            let k_properties = parser.name(&buf);
            if k_properties != "properties".as_bytes() { panic!(); }

            parser.begin_object();

            while parser.peek().kind != JsonTokenType::EndObject {

                let prop_key = parser.name(&buf);

                parser.begin_array();

                    while parser.peek().kind != JsonTokenType::EndArray {

                        parser.begin_object();

                            let mut value_datasources:Vec<&[u8]> = Vec::new();

                            // "datasources": ...
                            let k_value_datasources = parser.name(&buf);
                            if k_value_datasources != "datasources".as_bytes() { panic!(); }

                            parser.begin_array();
                                while parser.peek().kind != JsonTokenType::EndArray {
                                    value_datasources.push(parser.string(&buf));
                                }
                            parser.end_array();

                            // "value": ...
                            let k_value_value = parser.name(&buf);
                            if k_value_value != "value".as_bytes() { panic!(); }

                            let prop_value_kind = parser.peek().kind;
                            let prop_value = parser.value(&buf);

                            props.push(SlicedProperty { kind: prop_value_kind, key: prop_key, datasources: value_datasources, value: prop_value });

                        parser.end_object();
                    }

                parser.end_array();
            }
            parser.end_object();


        // }
        parser.end_object();


        return SlicedEntity {
            id: id.unwrap(),
            subjects,
            datasources,
            props };

    }


}

#[derive(Clone)]
pub struct SlicedReified<'a> {
    pub props:Vec<SlicedProperty<'a>>,
    pub value:&'a [u8]
}

impl<'a> SlicedReified<'a> {

     pub fn from_json(buf:&'a Vec<u8>) -> Option<SlicedReified<'a>> {

        // eprintln!("from_json: {:?}", String::from_utf8(buf.to_vec()).unwrap());

        let lexed = lex(&buf);
        let mut parser = JsonParser::from_lexed(lexed);

        let mut value:Option<&[u8]> = None;
        let mut props:Vec<SlicedProperty> = Vec::new();

        // {
        parser.begin_object();

            let k_value = parser.name(&buf);
            if k_value != "value".as_bytes() { return None; }
            value = Some(parser.value(&buf));

            // "properties": ...
            let k_properties = parser.name(&buf);
            if k_properties != "properties".as_bytes() { return None; }

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

        return Some(SlicedReified { props, value: value.unwrap() });
     }
}
