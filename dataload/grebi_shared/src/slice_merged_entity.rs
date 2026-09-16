

use std::collections::BTreeMap;

use crate::json_lexer::{lex, JsonToken, JsonTokenType};
use crate::json_parser::JsonParser;

#[derive(Clone)]
pub struct SlicedPropertyValue<'a> {
    pub kind:JsonTokenType,
    pub datasources:Vec<&'a [u8]>,
    pub source_ids:Vec<&'a [u8]>,
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
    pub source_ids:Vec<&'a [u8]>,
    pub subgraph:&'a [u8],
    pub props:Vec<SlicedProperty<'a>>,
    pub _refs:Option<&'a [u8]>,
    pub display_type:Option<&'a [u8]>,
    pub curie:Option<&'a [u8]>,
    pub model_id_to_embedding_vector:BTreeMap<&'a [u8], &'a [u8]>
}

impl<'a> SlicedEntity<'a> {

     pub fn from_json(buf:&'a Vec<u8>) -> SlicedEntity<'a> {

        let mut parser = JsonParser::parse(&buf);

        let mut props:Vec<SlicedProperty> = Vec::new();
        let mut entity_datasources:Vec<&[u8]> = Vec::new();
        let mut entity_source_ids:Vec<&[u8]> = Vec::new();
        let mut display_type:Option<&[u8]> = None;
        let mut curie:Option<&[u8]> = None;
        let mut _refs:Option<&[u8]> = None;
        let mut model_id_to_embedding_vector:BTreeMap<&'a [u8], &'a [u8]> = BTreeMap::new();
        
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

        // "grebi:sourceIds": ...
        let k_value_source_ids = parser.name();
        if k_value_source_ids != "grebi:sourceIds".as_bytes() { panic!("expected grebi:sourceIds as key, got {} in {}", String::from_utf8( k_value_source_ids.to_vec() ).unwrap(), String::from_utf8( buf.to_vec() ).unwrap()); }
        parser.begin_array();
            while parser.peek().kind != JsonTokenType::EndArray {
                entity_source_ids.push(parser.string());
            }
        parser.end_array();

        // "grebi:subgraph": ...
        let k_subgraph = parser.name();
        if k_subgraph != "grebi:subgraph".as_bytes() { panic!("expected grebi:subgraph as key, got {}", String::from_utf8( k_subgraph.to_vec() ).unwrap()); }
        let subgraph = parser.string();

        while parser.peek().kind != JsonTokenType::EndObject {

            let prop_key = parser.name();

            if prop_key == b"grebi:displayType" {
                display_type = Some(&parser.value());
                continue;
            }

            if prop_key == b"grebi:curie" {
                // Derived in the link stage (like grebi:displayType): a single
                // bare-string scalar, not a reified property array.
                curie = Some(parser.string());
                continue;
            }

            if prop_key.starts_with("embedding:".as_bytes()) {
                let prop_value = parser.value();
                let embedding_vector = Some(prop_value);
                let model_id = &prop_key["embedding:".len()..];
                model_id_to_embedding_vector.insert(model_id, embedding_vector.unwrap());
                continue;
            }

            if prop_key == b"_refs" {
                _refs = Some(&parser.value());
                continue;
            }

            let mut values:Vec<SlicedPropertyValue> = Vec::new();

            let values_slice_begin = parser.begin_array();

                while parser.peek().kind != JsonTokenType::EndArray {

                    parser.begin_object();

                        let mut value_datasources:Vec<&[u8]> = Vec::new();
                        let mut value_source_ids:Vec<&[u8]> = Vec::new();

                        // "grebi:datasources": ...
                        let k_value_datasources = parser.name();
                        if k_value_datasources != "grebi:datasources".as_bytes() { panic!(); }
                        parser.begin_array();
                            while parser.peek().kind != JsonTokenType::EndArray {
                                value_datasources.push(parser.string());
                            }
                        parser.end_array();

                        // "grebi:source_ids": ...
                        let k_value_source_ids = parser.name();
                        if k_value_source_ids != "grebi:sourceIds".as_bytes() { panic!(); }
                        parser.begin_array();
                            while parser.peek().kind != JsonTokenType::EndArray {
                                value_source_ids.push(parser.string());
                            }
                        parser.end_array();

                        // "grebi:value": ...
                        let k_value_value = parser.name();
                        if k_value_value != "grebi:value".as_bytes() { panic!(); }

                        let prop_value_kind = parser.peek().kind;
                        let prop_value = parser.value();

                        values.push(SlicedPropertyValue { kind: prop_value_kind, datasources: value_datasources, source_ids: value_source_ids, value: prop_value });

                    parser.end_object();
                }

            let values_slice_end = parser.end_array();

            props.push(SlicedProperty { key: prop_key, values, values_slice: &buf[values_slice_begin.index..values_slice_end.index+1] });
        }
        parser.end_object();

        return SlicedEntity { id, datasources: entity_datasources, source_ids: entity_source_ids, subgraph, props, display_type, curie, model_id_to_embedding_vector, _refs };

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

                        values.push(SlicedPropertyValue { kind, value: prop_value, datasources: Vec::new(), source_ids: Vec::new() });

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


#[cfg(test)]
mod tests {
    use super::*;

    const ENTITY: &str = concat!(
        r#"{"grebi:nodeId":"mondo:0005083","grebi:datasources":["OLS.mondo","GWAS"],"grebi:sourceIds":["mondo:0005083","efo:0000676"],"grebi:subgraph":"g1","#,
        r#""grebi:name":[{"grebi:datasources":["OLS.mondo"],"grebi:sourceIds":["mondo:0005083"],"grebi:value":"psoriasis"},{"grebi:datasources":["GWAS"],"grebi:sourceIds":["efo:0000676"],"grebi:value":"Psoriasis"}],"#,
        r#""grebi:displayType":"biolink:Disease","grebi:curie":"MONDO:0005083","embedding:m1":[0.1,0.2],"#,
        r#""count":[{"grebi:datasources":["GWAS"],"grebi:sourceIds":["efo:0000676"],"grebi:value":3}],"#,
        r#""nested":[{"grebi:datasources":["GWAS"],"grebi:sourceIds":[],"grebi:value":{"grebi:value":"x","grebi:properties":{"p":["q"]}}}],"#,
        r#""_refs":{"efo:0000676":{"grebi:name":["Psoriasis"]}}}"#
    );

    #[test]
    fn slices_a_merged_entity() {
        let buf = ENTITY.as_bytes().to_vec();
        let e = SlicedEntity::from_json(&buf);
        assert_eq!(e.id, b"mondo:0005083");
        assert_eq!(e.datasources, vec![b"OLS.mondo" as &[u8], b"GWAS"]);
        assert_eq!(e.source_ids, vec![b"mondo:0005083" as &[u8], b"efo:0000676"]);
        assert_eq!(e.subgraph, b"g1");
        assert_eq!(e.display_type, Some(br#""biolink:Disease""# as &[u8]), "the display type keeps its quotes");
        assert_eq!(e.curie, Some(b"MONDO:0005083" as &[u8]), "the curie does not");
        assert_eq!(e.model_id_to_embedding_vector.get(b"m1" as &[u8]), Some(&(b"[0.1,0.2]" as &[u8])));
        assert_eq!(e._refs, Some(br#"{"efo:0000676":{"grebi:name":["Psoriasis"]}}"# as &[u8]));

        let keys: Vec<&[u8]> = e.props.iter().map(|p| p.key).collect();
        assert_eq!(keys, vec![b"grebi:name" as &[u8], b"count", b"nested"], "the derived fields are not properties");

        let name = &e.props[0];
        assert_eq!(name.values.len(), 2);
        assert_eq!(name.values[0].kind, JsonTokenType::StartString);
        assert_eq!(name.values[0].value, br#""psoriasis""#);
        assert_eq!(name.values[0].datasources, vec![b"OLS.mondo" as &[u8]]);
        assert_eq!(name.values[0].source_ids, vec![b"mondo:0005083" as &[u8]]);
        assert_eq!(name.values[1].value, br#""Psoriasis""#);
        assert!(name.values_slice.starts_with(b"[{") && name.values_slice.ends_with(b"}]"), "the values slice is the whole array");

        assert_eq!(e.props[1].values[0].kind, JsonTokenType::StartNumber);
        assert_eq!(e.props[1].values[0].value, b"3");
        assert_eq!(e.props[2].values[0].kind, JsonTokenType::StartObject);
        assert_eq!(e.props[2].values[0].value, br#"{"grebi:value":"x","grebi:properties":{"p":["q"]}}"#);
    }

    #[test]
    fn an_entity_with_no_properties() {
        let buf = br#"{"grebi:nodeId":"a","grebi:datasources":[],"grebi:sourceIds":[],"grebi:subgraph":"g"}"#.to_vec();
        let e = SlicedEntity::from_json(&buf);
        assert_eq!(e.id, b"a");
        assert!(e.datasources.is_empty() && e.source_ids.is_empty() && e.props.is_empty());
        assert!(e.display_type.is_none() && e.curie.is_none() && e._refs.is_none());
        assert!(e.model_id_to_embedding_vector.is_empty());
    }

    #[test]
    #[should_panic(expected = "expected grebi:nodeId as key")]
    fn the_id_must_come_first() {
        let buf = br#"{"grebi:datasources":[],"grebi:nodeId":"a","grebi:sourceIds":[],"grebi:subgraph":"g"}"#.to_vec();
        SlicedEntity::from_json(&buf);
    }

    #[test]
    fn slices_a_reified_value() {
        let json: &[u8] = br#"{"grebi:value":"x","grebi:properties":{"p":["q",2],"r":[]}}"#;
        let r = SlicedReified::from_json(&json).expect("reified");
        assert_eq!(r.value, br#""x""#);
        assert_eq!(r.value_kind, JsonTokenType::StartString);
        assert_eq!(r.props.len(), 2);
        assert_eq!(r.props[0].key, b"p");
        assert_eq!(r.props[0].values.iter().map(|v| v.value).collect::<Vec<_>>(), vec![br#""q""# as &[u8], b"2"]);
        assert_eq!(r.props[0].values_slice, br#"["q",2]"#);
        assert!(r.props[1].values.is_empty());

        let json: &[u8] = br#"{"grebi:value":{"a":1},"grebi:properties":{}}"#;
        let r = SlicedReified::from_json(&json).unwrap();
        assert_eq!(r.value, br#"{"a":1}"#);
        assert_eq!(r.value_kind, JsonTokenType::StartObject);
    }

    #[test]
    fn objects_that_are_not_reified_values_give_none() {
        assert!(SlicedReified::from_json(&(b"{}" as &[u8])).is_none());
        assert!(SlicedReified::from_json(&(br#"{"a":1}"# as &[u8])).is_none());
        assert!(SlicedReified::from_json(&(br#"{"grebi:value":1,"other":2}"# as &[u8])).is_none());
    }
}
