
use crate::json_lexer::{lex, JsonToken, JsonTokenType};
use crate::json_parser::JsonParser;
use crate::slice_merged_entity::{SlicedProperty, SlicedPropertyValue};

#[derive(Clone)]
pub struct SlicedEdge<'a> {
    pub edge_id:&'a [u8],
    pub edge_type:&'a [u8],
    pub subgraph:&'a [u8],
    pub from:&'a [u8],
    pub to:&'a [u8],
    pub datasources:Vec<&'a [u8]>,
    pub props:Vec<SlicedProperty<'a>>,
    pub _refs:Option<&'a [u8]>
}

impl<'a> SlicedEdge<'a> {

     pub fn from_json(buf:&'a Vec<u8>) -> SlicedEdge<'a> {

        let mut parser = JsonParser::parse(&buf);

        let mut props:Vec<SlicedProperty> = Vec::new();
        let mut entity_datasources:Vec<&[u8]> = Vec::new();
        let mut _refs:Option<&[u8]> = None;
        
        // {
        parser.begin_object();

        // "edge_id": ...
        let k_edge_id = parser.name();
        if k_edge_id != "grebi:edgeId".as_bytes() { panic!("expected edge_id as key, got {}", String::from_utf8( k_edge_id.to_vec() ).unwrap()); }
        let edge_id = parser.string();

        // "type": ...
        let k_type = parser.name();
        if k_type != "grebi:type".as_bytes() { panic!("expected type as key, got {}", String::from_utf8( k_type.to_vec() ).unwrap()); }
        let edge_type = parser.string();

        // "subgraph": ...
        let k_subgraph = parser.name();
        if k_subgraph != "grebi:subgraph".as_bytes() { panic!("expected subgraph as key, got {}", String::from_utf8( k_subgraph.to_vec() ).unwrap()); }
        let edge_subgraph = parser.string();

        // "from": ...
        let k_from = parser.name();
        if k_from != "grebi:from".as_bytes() { panic!("expected from as key, got {}", String::from_utf8( k_from.to_vec() ).unwrap()); }
        let from = parser.string();

        // "to": ...
        let k_to = parser.name();
        if k_to != "grebi:to".as_bytes() { panic!("expected to as key, got {}", String::from_utf8( k_to.to_vec() ).unwrap()); }
        let to = parser.string();

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

            if prop_key == b"_refs" {
                _refs = Some(&parser.value());
                continue;
            }

            let mut values:Vec<SlicedPropertyValue> = Vec::new();

            let values_slice_begin = parser.begin_array();

                while parser.peek().kind != JsonTokenType::EndArray {

                    let prop_value_kind = parser.peek().kind;
                    let prop_value = parser.value();

                    values.push(SlicedPropertyValue { kind: prop_value_kind, datasources: vec![], value: prop_value });
                }

            let values_slice_end = parser.end_array();

            props.push(SlicedProperty { key: prop_key, values, values_slice: &buf[values_slice_begin.index..values_slice_end.index] });
        }

        parser.end_object();


        return SlicedEdge { edge_id, edge_type, subgraph: edge_subgraph, from, to, datasources: entity_datasources, props, _refs };
    }


}
