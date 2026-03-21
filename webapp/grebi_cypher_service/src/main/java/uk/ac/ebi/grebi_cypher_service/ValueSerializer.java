package uk.ac.ebi.grebi_cypher_service;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

class ValueSerializer {

    // ---------------------------------------------------------------
    //  Embedded (org.neo4j.graphdb.*)
    // ---------------------------------------------------------------

    static Object serializeEmbeddedValue(Object value) {
        if (value == null) return null;

        if (value instanceof org.neo4j.graphdb.Node node) {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("_labels", StreamSupport.stream(node.getLabels().spliterator(), false)
                    .map(org.neo4j.graphdb.Label::name)
                    .collect(Collectors.toList()));
            for (var entry : node.getAllProperties().entrySet()) {
                map.put(entry.getKey(), serializeEmbeddedValue(entry.getValue()));
            }
            return map;
        }

        if (value instanceof org.neo4j.graphdb.Relationship rel) {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("_type", rel.getType().name());
            for (var entry : rel.getAllProperties().entrySet()) {
                map.put(entry.getKey(), serializeEmbeddedValue(entry.getValue()));
            }
            return map;
        }

        if (value instanceof List<?> list) {
            return list.stream().map(ValueSerializer::serializeEmbeddedValue).collect(Collectors.toList());
        }

        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            map.forEach((k, v) -> result.put(k.toString(), serializeEmbeddedValue(v)));
            return result;
        }

        if (value instanceof byte[] bytes) {
            return Base64.getEncoder().encodeToString(bytes);
        }

        return value; // primitives: String, Long, Double, Boolean
    }

    // ---------------------------------------------------------------
    //  Bolt (org.neo4j.driver.Value)
    // ---------------------------------------------------------------

    static Object serializeBoltValue(org.neo4j.driver.Value value) {
        if (value == null || value.isNull()) return null;
        return serializeBoltObject(value.asObject());
    }

    static Object serializeBoltObject(Object obj) {
        if (obj == null) return null;

        if (obj instanceof org.neo4j.driver.types.Node node) {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("_labels", StreamSupport.stream(node.labels().spliterator(), false)
                    .collect(Collectors.toList()));
            node.asMap().forEach((k, v) -> map.put(k, serializeBoltObject(v)));
            return map;
        }

        if (obj instanceof org.neo4j.driver.types.Relationship rel) {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("_type", rel.type());
            rel.asMap().forEach((k, v) -> map.put(k, serializeBoltObject(v)));
            return map;
        }

        if (obj instanceof List<?> list) {
            return list.stream().map(ValueSerializer::serializeBoltObject).collect(Collectors.toList());
        }

        if (obj instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            map.forEach((k, v) -> result.put(k.toString(), serializeBoltObject(v)));
            return result;
        }

        if (obj instanceof byte[] bytes) {
            return Base64.getEncoder().encodeToString(bytes);
        }

        return obj;
    }

    // ---------------------------------------------------------------
    //  Parameter conversion — Gson deserializes all numbers as Double;
    //  Neo4j expects Long for integer params.
    // ---------------------------------------------------------------

    @SuppressWarnings("unchecked")
    static Map<String, Object> convertParams(Map<String, Object> params) {
        Map<String, Object> converted = new LinkedHashMap<>();
        for (var entry : params.entrySet()) {
            converted.put(entry.getKey(), convertParamValue(entry.getValue()));
        }
        return converted;
    }

    @SuppressWarnings("unchecked")
    static Object convertParamValue(Object value) {
        if (value instanceof Double d) {
            if (d == Math.floor(d) && !Double.isInfinite(d)) {
                return d.longValue();
            }
            return d;
        }
        if (value instanceof List<?> list) {
            return list.stream().map(ValueSerializer::convertParamValue).collect(Collectors.toList());
        }
        if (value instanceof Map<?, ?> map) {
            return convertParams((Map<String, Object>) map);
        }
        return value;
    }
}
