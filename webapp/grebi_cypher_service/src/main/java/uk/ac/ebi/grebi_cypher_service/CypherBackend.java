package uk.ac.ebi.grebi_cypher_service;

import java.io.OutputStream;
import java.util.Map;

interface CypherBackend extends AutoCloseable {
    String getSubgraph();
    void streamQuery(String query, Map<String, Object> params, OutputStream out) throws Exception;
}
