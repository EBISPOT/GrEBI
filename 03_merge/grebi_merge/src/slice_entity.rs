

use grebi_shared::json_lexer::{lex, JsonTokenType};
use grebi_shared::json_parser::JsonParser;

#[derive(Clone)]
pub struct SlicedProperty<'a> {
    pub key:&'a [u8],
    pub value:&'a [u8]
}

#[derive(Clone)]
pub struct SlicedEntity<'a> {
    pub id:&'a [u8],
    pub subjects_block:&'a [u8],
    pub subjects:Vec<&'a [u8]>,
    pub datasource:&'a [u8],
    pub props:Vec<SlicedProperty<'a>>
}

impl<'a> SlicedEntity<'a> {

     pub fn from_json(buf:&'a Vec<u8>) -> SlicedEntity<'a> {

        let lexed = lex(&buf);

        let mut parser = JsonParser::from_lexed(lexed);

        let mut subjects:Vec<&[u8]> = Vec::new();
        let mut props:Vec<SlicedProperty> = Vec::new();

        // {
        parser.begin_object();

            // "id": ...
            let k_id = parser.name(&buf);
            if k_id != "id".as_bytes() { panic!(); }
            let id = parser.string(&buf);

            // "subjects": ...
            let k_subjects = parser.name(&buf);
            if k_subjects != "subjects".as_bytes() { panic!(); }
            let subj_block_begin = parser.begin_array();
                while parser.peek().kind != JsonTokenType::EndArray {
                    subjects.push(parser.string(&buf));
                }
            let subj_block_end = parser.end_array();
            let subjects_block = &buf[subj_block_begin.index..subj_block_end.index+1];

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


        return SlicedEntity { id, subjects_block, subjects, datasource, props };

    }


}
