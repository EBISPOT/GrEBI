package uk.ac.ebi.grebi_cypher_service;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

class EmbeddedBackend implements CypherBackend {

    private final org.neo4j.dbms.api.DatabaseManagementService dbms;
    private final org.neo4j.graphdb.GraphDatabaseService db;
    private final String graph;
    private final Path cleanHome;

    EmbeddedBackend(Path homeDir, long pageCacheMb) {
        System.out.println("Opening embedded Neo4j database at " + homeDir
                + " with page cache " + pageCacheMb + " MB");

        // The builder reads conf/neo4j.conf from the home directory.
        // Server-produced databases have server settings (bolt, http, etc.)
        // that we don't want in embedded mode. Create a clean home with
        // only a symlink to data/ so no conf file is loaded.
        try {
            cleanHome = Files.createTempDirectory("grebi_neo4j_");
            Files.createSymbolicLink(cleanHome.resolve("data"),
                    homeDir.resolve("data").toAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create clean home for " + homeDir, e);
        }

        var builder = new org.neo4j.dbms.api.DatabaseManagementServiceBuilder(cleanHome)
                .setConfig(org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default, true);
        if (pageCacheMb > 0) {
            builder.setConfig(
                org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory,
                pageCacheMb * 1024 * 1024);
        }
        dbms = builder.build();
        db = dbms.database("neo4j");

        // Discover which graph this database contains
        try (org.neo4j.graphdb.Transaction tx = db.beginTx()) {
            org.neo4j.graphdb.Result result =
                    tx.execute("MATCH (n:GraphNode) RETURN n.`grebi:subgraph` AS graph LIMIT 1");
            if (result.hasNext()) {
                graph = (String) result.next().get("graph");
            } else {
                throw new RuntimeException("No GraphNode found in " + homeDir + " — cannot determine graph");
            }
            tx.commit();
        }

        System.out.println("Loaded embedded graph '" + graph + "' from " + homeDir);
    }

    long countNodes() {
        try (org.neo4j.graphdb.Transaction tx = db.beginTx()) {
            var result = tx.execute("MATCH (n) RETURN count(n) AS c");
            long count = (Long) result.next().get("c");
            tx.commit();
            return count;
        }
    }

    @Override
    public String getGraph() {
        return graph;
    }

    @Override
    public void streamQuery(String query, Map<String, Object> params, OutputStream out) throws Exception {
        try (org.neo4j.graphdb.Transaction tx = db.beginTx()) {
            org.neo4j.graphdb.Result result = tx.execute(query, params);
            while (result.hasNext()) {
                Map<String, Object> record = result.next();
                Map<String, Object> serialized = new LinkedHashMap<>();
                for (var entry : record.entrySet()) {
                    serialized.put(entry.getKey(), ValueSerializer.serializeEmbeddedValue(entry.getValue()));
                }
                out.write(GrebiCypherSvc.gson.toJson(serialized).getBytes(StandardCharsets.UTF_8));
                out.write('\n');
                out.flush();
            }
            tx.commit();
        }
    }

    @Override
    public void close() {
        dbms.shutdown();
        try {
            Files.deleteIfExists(cleanHome.resolve("data"));
            Files.deleteIfExists(cleanHome);
        } catch (IOException e) {
            // best-effort cleanup
        }
    }
}
