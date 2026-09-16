package uk.ac.ebi.grebi;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** The graph-level and metadata routes. */
class GrebiApiRoutesTest {

    TestApp app;

    @BeforeEach
    void start() {
        app = TestApp.start();
    }

    @AfterEach
    void stop() {
        app.close();
    }

    @Test
    void healthAndGraphs() {
        var health = app.get("/api/health");
        assertEquals(200, health.status());
        assertEquals("ok", health.json().getAsJsonObject().get("status").getAsString());
        assertEquals("application/json", health.header("content-type"));

        var graphs = app.get("/api/v1/graphs");
        assertEquals(List.of("g1", "g2"), TestApp.GSON.fromJson(graphs.body(), List.class));
    }

    @Test
    void statsComeFromTheCypherService() {
        var stats = app.get("/api/v1/stats").json().getAsJsonObject();
        assertEquals(42, stats.getAsJsonObject("g1").get("num_nodes").getAsInt());
    }

    @Test
    void statsSayWhenTheCypherServiceIsMissing() {
        try (var noCypher = TestApp.startWithoutCypher()) {
            var res = noCypher.get("/api/v1/stats");
            assertEquals(200, res.status());
            assertEquals("cypher service is not available", res.json().getAsJsonObject().get("error").getAsString());
        }
    }

    @Test
    void topicsComeFromTheTemplatesDirectory() {
        var topics = app.get("/api/v1/topics").json().getAsJsonArray();
        assertEquals(2, topics.size());
        assertEquals("gwas", topics.get(0).getAsJsonObject().get("id").getAsString());
        assertEquals("GWAS Catalog", topics.get(0).getAsJsonObject().get("name").getAsString());
    }

    @Test
    void graphMetadataLeavesOutTheBulkKeysUnlessAskedForEverything() {
        var summary = app.get("/api/v1/graphs/g1").json().getAsJsonObject();
        assertEquals("g1", summary.get("subgraph_name").getAsString());
        assertTrue(summary.has("types"));
        assertTrue(summary.has("materialised_templates"));
        for (String bulk : GrebiApi.BULK_METADATA_KEYS) {
            assertFalse(summary.has(bulk), bulk + " should be omitted from the summary");
        }

        var full = app.get("/api/v1/graphs/g1?full=true").json().getAsJsonObject();
        assertTrue(full.has("edges"));
        assertTrue(full.has("entity_props"));
        assertFalse(full.has("embedding_pca_models"), "the PCA models are never served");

        // ?full=false is the summary again
        assertFalse(app.get("/api/v1/graphs/g1?full=false").json().getAsJsonObject().has("edges"));
    }

    @Test
    void graphStatsAreDerivedFromTheRecordedMetadata() {
        JsonObject stats = app.get("/api/v1/graphs/g1/stats").json().getAsJsonObject();
        assertEquals(Map.of("biolink:Disease", 10.0, "gwas:Study", 5.0),
            TestApp.GSON.fromJson(stats.get("node_counts_by_type"), Map.class));
        assertEquals(Map.of("GWAS", 5.0, "OLS.efo", 10.0),
            TestApp.GSON.fromJson(stats.get("node_counts_by_datasource"), Map.class));
        // edge counts sum every destination type and datasource signature
        assertEquals(Map.of("gwas:associated_with", 10.0, "biolink:subclass_of", 20.0),
            TestApp.GSON.fromJson(stats.get("edge_counts_by_type"), Map.class));
        // a "GWAS,OLS.efo" signature counts towards both datasources
        assertEquals(Map.of("GWAS", 10.0, "OLS.efo", 23.0),
            TestApp.GSON.fromJson(stats.get("edge_counts_by_datasource"), Map.class));

        var empty = app.get("/api/v1/graphs/g2/stats").json().getAsJsonObject();
        assertEquals(0, empty.getAsJsonObject("node_counts_by_type").size());
        assertEquals(0, empty.getAsJsonObject("edge_counts_by_type").size());
    }

    @Test
    void collectionsIsAnEmptyObject() {
        assertEquals("{}", app.get("/api/v1/collections").body());
    }
}
