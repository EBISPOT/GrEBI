

use crate::json_lexer::{lex, JsonTokenType};
use crate::json_parser::JsonParser;

#[derive(Clone)]
pub struct SlicedProperty<'a> {
    pub key:&'a [u8],
    pub values:Vec<&'a [u8]>
}

#[derive(Clone)]
pub struct SlicedEntity<'a> {
    pub subjects_block:&'a [u8],
    pub subjects:Vec<&'a [u8]>,
    pub datasources:Vec<&'a [u8]>,
    pub props:Vec<SlicedProperty<'a>>
}

impl<'a> SlicedEntity<'a> {

     pub fn from_json(buf:&'a Vec<u8>) -> SlicedEntity<'a> {

        let lexed = lex(&buf);

        let mut parser = JsonParser::from_lexed(lexed);

        let mut subjects:Vec<&[u8]> = Vec::new();
        let mut datasources:Vec<&[u8]> = Vec::new();
        let mut props:Vec<SlicedProperty> = Vec::new();
        let mut subjects_block:Option<&[u8]> = None;

        // {
        parser.begin_object();

            // "subjects": ...
            let k_subjects = parser.name(&buf);
            if k_subjects != "subjects".as_bytes() { panic!(); }
            parser.begin_array();
            let subj_block_begin = parser.get_index();
                while parser.peek().kind != JsonTokenType::EndArray {
                    subjects.push(parser.string(&buf));
                }
            let subj_block_end = parser.get_index();
            parser.end_array();
            subjects_block = Some(&buf[subj_block_begin..subj_block_end]);

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

                let mut values:Vec<&[u8]> = Vec::new();

                parser.begin_array();

                    while parser.peek().kind != JsonTokenType::EndArray {

                        let prop_value = parser.value(&buf);

                        values.push(prop_value);

                    }

                parser.end_array();

                props.push(SlicedProperty { key: prop_key, values });
            }
            parser.end_object();


        // }
        parser.end_object();


        return SlicedEntity {
            subjects_block: subjects_block.unwrap(),
            subjects,
            datasources,
            props };

    }


}

