

use grebi_shared::json_lexer::{lex, JsonToken, JsonTokenType};
use grebi_shared::json_parser::JsonParser;

#[derive(Clone)]
pub struct SlicedProperty<'a> {
    pub key:&'a [u8],
    pub value:&'a [u8]
}

#[derive(Clone)]
pub struct SlicedEntity<'a> {
    pub subject:&'a [u8],
    pub datasources:Vec<&'a [u8]>,
    pub props:Vec<SlicedProperty<'a>>
}

impl<'a> SlicedEntity<'a> {

     pub fn from_json(buf:&'a Vec<u8>) -> SlicedEntity<'a> {

        let lexed = lex(&buf);

        let mut parser = JsonParser::from_lexed(lexed);

        let mut subject:Option<&[u8]> = None;
        let mut datasources:Vec<&[u8]> = Vec::new();
        let mut props:Vec<SlicedProperty> = Vec::new();

        // {
        parser.begin_object();

            // "subject": ...
            let k_subject = parser.name(&buf);
            if k_subject != "subject".as_bytes() { panic!("expected subject as key, got {}", String::from_utf8( k_subject.to_vec() ).unwrap()); }
            subject = Some(parser.string(&buf));

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

                        let prop_value = parser.value(&buf);

                        props.push(SlicedProperty { key: prop_key, value: prop_value });

                    }

                parser.end_array();
            }
            parser.end_object();


        // }
        parser.end_object();


        return SlicedEntity {
            subject: subject.unwrap(),
            datasources,
            props };

    }


}

