
use grebi_shared::json_lexer::{lex, JsonTokenType};
use grebi_shared::json_parser::JsonParser;

#[derive(Clone)]
pub struct SlicedProperty<'a> {
    pub key:&'a [u8],
    pub values:Vec<&'a [u8]>
}

#[derive(Clone)]
pub struct SlicedEntity<'a> {
    pub subject:&'a [u8],
    pub datasource:&'a [u8],
    pub props:Vec<SlicedProperty<'a>>
}

impl<'a> SlicedEntity<'a> {

     pub fn from_json(buf:&'a Vec<u8>) -> SlicedEntity<'a> {

        let lexed = lex(&buf);

        let mut parser = JsonParser::from_lexed(lexed);
        let mut props:Vec<SlicedProperty> = Vec::new();

        // {
        parser.begin_object();

            // "subject": ...
            let k_subject = parser.name(&buf);
            if k_subject != "subject".as_bytes() { panic!(); }
            let subject = parser.string(&buf);

            // "datasource": ...
            let k_datasource = parser.name(&buf);
            if k_datasource != "datasource".as_bytes() { panic!(); }
            let datasource = parser.string(&buf);

            // "properties": ...
            let k_properties = parser.name(&buf);
            if k_properties != "properties".as_bytes() { panic!(); }

            parser.begin_object();

            while parser.peek().kind != JsonTokenType::EndObject {

                let prop_key = parser.name(&buf);

                let mut values:Vec<&[u8]> = Vec::new();

                parser.begin_array();
                    while parser.peek().kind != JsonTokenType::EndArray {
                        values.push(parser.value(&buf));
                    }
                parser.end_array();

                props.push(SlicedProperty { key: prop_key, values });
            }
            parser.end_object();

        // }
        parser.end_object();

        return SlicedEntity {
            subject: subject,
            datasource: datasource,
            props };

    }


}

