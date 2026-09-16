
use crate::json_lexer::{lex, JsonToken, JsonTokenType};
use crate::json_parser::JsonParser;
use crate::slice_merged_entity::{SlicedProperty, SlicedPropertyValue};

#[derive(Clone)]
pub struct SlicedEdge<'a> {
    pub edge_id:&'a [u8],
    pub edge_type:&'a [u8],
    pub subgraph:&'a [u8],
    pub from_node_id:&'a [u8],
    pub from_source_ids:Vec<&'a [u8]>,
    pub to_node_id:&'a [u8],
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

        // "fromNodeId": ...
        let k_from_nid = parser.name();
        if k_from_nid != "grebi:fromNodeId".as_bytes() { panic!("expected from as key, got {}", String::from_utf8( k_from_nid.to_vec() ).unwrap()); }
        let from_nid = parser.string();

        // "fromSourceIds": ...
        let k_from_sids = parser.name();
        if k_from_sids != "grebi:fromSourceIds".as_bytes() { panic!("expected from as key, got {}", String::from_utf8( k_from_sids.to_vec() ).unwrap()); }
        parser.begin_array();
        let mut from_sids:Vec<&[u8]> = Vec::new();
        while parser.peek().kind != JsonTokenType::EndArray {
            from_sids.push(parser.string());
        }
        parser.end_array();

        // "toNodeId": ...
        let k_to_nid = parser.name();
        if k_to_nid != "grebi:toNodeId".as_bytes() { panic!("expected to as key, got {}", String::from_utf8( k_to_nid.to_vec() ).unwrap()); }
        let to_nid = parser.string();

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

                    values.push(SlicedPropertyValue { kind: prop_value_kind, datasources: vec![], value: prop_value, source_ids: vec![] });
                }

            let values_slice_end = parser.end_array();

            props.push(SlicedProperty { key: prop_key, values, values_slice: &buf[values_slice_begin.index..values_slice_end.index] });
        }

        parser.end_object();


        return SlicedEdge {
            edge_id,
            edge_type,
            subgraph: edge_subgraph,
            from_node_id: from_nid,
            from_source_ids: from_sids,
            to_node_id: to_nid,
            datasources: entity_datasources,
            props,
            _refs
        };
    }


}


#[cfg(test)]
mod tests {
    use super::*;

    const EDGE: &str = concat!(
        r#"{"grebi:edgeId":"e1","grebi:type":"biolink:subclass_of","grebi:subgraph":"g1","grebi:fromNodeId":"a","grebi:fromSourceIds":["efo:1","mondo:1"],"grebi:toNodeId":"b","grebi:datasources":["OLS.efo"],"#,
        r#""gwas:p_value":[1e-8,"x"],"_refs":{"a":{"grebi:name":["A"]}}}"#
    );

    #[test]
    fn slices_a_materialised_edge() {
        let buf = EDGE.as_bytes().to_vec();
        let e = SlicedEdge::from_json(&buf);
        assert_eq!(e.edge_id, b"e1");
        assert_eq!(e.edge_type, b"biolink:subclass_of");
        assert_eq!(e.subgraph, b"g1");
        assert_eq!(e.from_node_id, b"a");
        assert_eq!(e.from_source_ids, vec![b"efo:1" as &[u8], b"mondo:1"]);
        assert_eq!(e.to_node_id, b"b");
        assert_eq!(e.datasources, vec![b"OLS.efo" as &[u8]]);
        assert_eq!(e._refs, Some(br#"{"a":{"grebi:name":["A"]}}"# as &[u8]));
        assert_eq!(e.props.len(), 1);
        assert_eq!(e.props[0].key, b"gwas:p_value");
        assert_eq!(e.props[0].values[0].kind, JsonTokenType::StartNumber);
        assert_eq!(e.props[0].values[0].value, b"1e-8");
        assert_eq!(e.props[0].values[1].value, br#""x""#);
        // NB: unlike an entity's, an edge's values slice stops before the closing bracket
        assert_eq!(e.props[0].values_slice, br#"[1e-8,"x""#);
    }

    #[test]
    fn an_edge_with_no_extra_properties() {
        let buf = br#"{"grebi:edgeId":"e","grebi:type":"t","grebi:subgraph":"g","grebi:fromNodeId":"a","grebi:fromSourceIds":[],"grebi:toNodeId":"b","grebi:datasources":[]}"#.to_vec();
        let e = SlicedEdge::from_json(&buf);
        assert!(e.props.is_empty() && e._refs.is_none() && e.from_source_ids.is_empty());
    }

    #[test]
    #[should_panic(expected = "expected edge_id as key")]
    fn the_edge_id_must_come_first() {
        let buf = br#"{"grebi:type":"t","grebi:edgeId":"e"}"#.to_vec();
        SlicedEdge::from_json(&buf);
    }
}
