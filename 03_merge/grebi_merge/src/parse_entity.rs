

use grebi_shared::json_lexer::{lex, JsonTokenType};
use grebi_shared::json_parser::JsonParser;

#[derive(Clone)]
pub struct ParsedProperty<'a> {
    pub key:&'a [u8],
    pub value:&'a [u8]
}

#[derive(Clone)]
pub struct ParsedEntity<'a> {
    pub id:&'a [u8],
    pub source_ids:Vec<&'a [u8]>,
    pub props:Vec<ParsedProperty<'a>>,
    pub datasource:&'a [u8],
    pub has_type:bool
}

impl<'a> ParsedEntity<'a> {

     pub fn from_json(buf:&'a Vec<u8>, datasource:&'a [u8]) -> ParsedEntity<'a> {

        let mut parser = JsonParser::parse(&buf);

        let mut props:Vec<ParsedProperty> = Vec::new();
        let mut has_type = false;
        let mut ds:&[u8] = datasource;
        let mut source_ids:Vec<&[u8]> = Vec::new();

        // {
        parser.begin_object();

            let k_id = parser.name();
            if k_id != "grebi:nodeId".as_bytes() { panic!(); }
            let id = parser.string();

            let k_sourceIds = parser.name();
            if k_sourceIds != "grebi:sourceIds".as_bytes() { panic!(); }
            while parser.peek().kind != JsonTokenType::EndArray {
                source_ids.push(parser.string());
            }

            while parser.peek().kind != JsonTokenType::EndObject {

                let prop_key = parser.name();

                if prop_key == "grebi:type".as_bytes() {
                    has_type = true;
                } else if prop_key == "grebi:datasource".as_bytes() {
                    let prop_value = parser.string();
                    ds = prop_value;
                    continue;
                }

                // All property values will be arrays in the merged output
                // So we intentionally don't care if it's an array or not here and
                // put it in the same list for the merger to deal with more easily.
                //
                if parser.peek().kind == JsonTokenType::StartArray {
                    parser.begin_array();
                    while parser.peek().kind != JsonTokenType::EndArray {
                        let prop_value = parser.value();
                        props.push(ParsedProperty { key: prop_key, value: prop_value });
                    }
                    parser.end_array();
                } else {
                    let prop_value = parser.value();
                    props.push(ParsedProperty { key: prop_key, value: prop_value });
                }
            }

        // }
        parser.end_object();


        return ParsedEntity { id, source_ids, props, datasource: ds, has_type };

    }


}
