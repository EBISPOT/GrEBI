package uk.ac.ebi.grebi_cypher_service;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

class BoltBackend implements CypherBackend {

    private final org.neo4j.driver.Driver driver;
    private final String graph;

    private static final long QUERY_TIMEOUT_SECONDS =
        Long.parseLong(System.getenv().getOrDefault("GREBI_QUERY_TIMEOUT_SECONDS", "120"));

    BoltBackend(String boltUrl) {
        System.out.println("Connecting to Neo4j via bolt at " + boltUrl);

        driver = org.neo4j.driver.GraphDatabase.driver(boltUrl);

        try (var session = driver.session(org.neo4j.driver.SessionConfig.builder()
                .withDatabase("neo4j")
                .withDefaultAccessMode(org.neo4j.driver.AccessMode.READ)
                .build())) {
            var result = session.run(
                    "MATCH (n:GraphNode) RETURN n.`grebi:subgraph` AS graph LIMIT 1");
            graph = result.single().get("graph").asString();
        }

        System.out.println("Loaded bolt graph '" + graph + "' from " + boltUrl);
    }

    @Override
    public String getGraph() {
        return graph;
    }

    @Override
    public void streamQuery(String query, Map<String, Object> params, OutputStream rawOut) throws Exception {
        BufferedOutputStream out = new BufferedOutputStream(rawOut, 64 * 1024);
        var txConfig = org.neo4j.driver.TransactionConfig.builder()
                .withTimeout(Duration.ofSeconds(QUERY_TIMEOUT_SECONDS))
                .build();
        try (var session = driver.session(org.neo4j.driver.SessionConfig.builder()
                .withDatabase("neo4j")
                .withDefaultAccessMode(org.neo4j.driver.AccessMode.READ)
                .build())) {
            var result = session.run(new org.neo4j.driver.Query(query, params), txConfig);
            while (result.hasNext()) {
                var record = result.next();
                Map<String, Object> serialized = new LinkedHashMap<>();
                for (var key : record.keys()) {
                    serialized.put(key, ValueSerializer.serializeBoltValue(record.get(key)));
                }
                out.write(GrebiCypherSvc.gson.toJson(serialized).getBytes(StandardCharsets.UTF_8));
                out.write('\n');
            }
        } finally {
            out.flush();
        }
    }

    @Override
    public void close() {
        driver.close();
    }
}
