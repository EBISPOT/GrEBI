package uk.ac.ebi.grebi;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import uk.ac.ebi.grebi.db.EmbeddingServiceClient;
import uk.ac.ebi.grebi.db.GrebiPostgresClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** The edge routes and the embedding-backed search routes. */
class GrebiApiEdgeAndVectorRoutesTest {

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
    @SuppressWarnings("unchecked")
    void edgeSearchPassesFiltersAndSort() {
        when(app.postgres.searchEdges(eq("g1"), any(), anyString(), anyString(), any()))
            .thenReturn(TestApp.facetedPage(List.of(TestApp.row("grebi:edgeId", "e1")), Map.of(), 1));

        var res = app.get("/api/v1/graphs/g1/edges?grebi:type=gwas:associated_with&sortBy=gwas:p_value&sortDir=desc&page=2&size=4");
        assertEquals(200, res.status());
        assertEquals("e1", res.json().getAsJsonObject().getAsJsonArray("content").get(0).getAsJsonObject().get("grebi:edgeId").getAsString());
        var filters = ArgumentCaptor.forClass(Map.class);
        verify(app.postgres).searchEdges(eq("g1"), filters.capture(), eq("gwas:p_value"), eq("desc"),
            argThat(p -> p.getPageNumber() == 2 && p.getPageSize() == 4));
        assertEquals(Map.of("grebi:type", List.of("gwas:associated_with")), filters.getValue());
    }

    @Test
    void anEdgeIsLookedUpWithoutItsGraphPrefixAndGetsItsEndsAttached() {
        var edge = TestApp.row("grebi:edgeId", "e1", "grebi:fromNodeId", "a", "grebi:toNodeId", "b",
            "_refs", Map.of("a", Map.of("grebi:name", List.of("A")), "b", Map.of("grebi:name", List.of("B"))));
        when(app.pgClient.resolveToMap("g1", List.of("e1"))).thenReturn(Map.of("e1", edge));

        var res = app.get("/api/v1/graphs/g1/edges/" + TestApp.encodeId("g1:e1"));
        assertEquals(200, res.status());
        var body = res.json().getAsJsonObject();
        assertEquals("A", body.getAsJsonObject("from").getAsJsonArray("grebi:name").get(0).getAsString());
        assertEquals("B", body.getAsJsonObject("to").getAsJsonArray("grebi:name").get(0).getAsString());

        // the id works without the prefix too, and an unknown edge is a 404
        assertEquals(200, app.get("/api/v1/graphs/g1/edges/" + TestApp.encodeId("e1")).status());
        var missing = app.get("/api/v1/graphs/g1/edges/" + TestApp.encodeId("nope"));
        assertEquals(404, missing.status());
        assertEquals("Edge not found", missing.json().getAsJsonObject().get("error").getAsString());
    }

    @Test
    void embeddingRoutesWithoutAModelForTheGraph() {
        assertEquals("[]", app.get("/api/v1/graphs/g1/embedding_models").body());

        assertEquals("q parameter is required", app.get("/api/v1/graphs/g1/semantic_search").json().getAsJsonObject().get("error").getAsString());
        assertEquals(400, app.get("/api/v1/graphs/g1/semantic_search").status());
        assertEquals("model parameter is required", app.get("/api/v1/graphs/g1/semantic_search?q=x").json().getAsJsonObject().get("error").getAsString());
        var noModels = app.get("/api/v1/graphs/g1/semantic_search?q=x&model=m1");
        assertEquals(400, noModels.status());
        assertEquals("No embedding models available for this graph", noModels.json().getAsJsonObject().get("error").getAsString());

        var similar = app.get("/api/v1/graphs/g1/nodes/" + TestApp.encodeId("n1") + "/similar");
        assertEquals(404, similar.status());
        assertEquals("No embedding models available for this graph", similar.json().getAsJsonObject().get("error").getAsString());
    }

    @Test
    void semanticSearchEmbedsTheQueryAndOptionallyResolvesTheHits() throws Exception {
        var client = mock(EmbeddingServiceClient.class);
        when(client.getAvailableModels()).thenReturn(List.of("m1", "m2"));
        when(client.getEmbeddableModels()).thenReturn(Set.of("m1"));
        when(client.embedText("m1", "psoriasis")).thenReturn(new float[] {0.1f, 0.2f});
        app.embeddingClients.put("g1", client);
        when(app.postgres.searchByVector(eq("g1"), eq("m1"), any(), eq(5)))
            .thenReturn(List.of(new GrebiPostgresClient.VectorSearchResult("n1", "N1", List.of("GWAS"), List.of("t"), List.of("s1"), 0.25)));

        var models = app.get("/api/v1/graphs/g1/embedding_models").json().getAsJsonArray();
        assertEquals(2, models.size());
        assertEquals("m1", models.get(0).getAsJsonObject().get("model").getAsString());
        assertTrue(models.get(0).getAsJsonObject().get("can_embed").getAsBoolean());
        assertFalse(models.get(1).getAsJsonObject().get("can_embed").getAsBoolean());

        var raw = app.get("/api/v1/graphs/g1/semantic_search?q=psoriasis&model=m1&n=5").json().getAsJsonArray();
        assertEquals("n1", raw.get(0).getAsJsonObject().get("nodeId").getAsString());
        assertEquals(0.25, raw.get(0).getAsJsonObject().get("distance").getAsDouble(), 1e-9);

        var resolvedNode = new HashMap<String, Object>(Map.of("grebi:nodeId", "n1", "grebi:name", List.of("N1 full")));
        when(app.pgClient.resolveToMap("g1", List.of("n1"))).thenReturn(Map.of("n1", resolvedNode));
        var resolved = app.get("/api/v1/graphs/g1/semantic_search?q=psoriasis&model=m1&n=5&resolve=true").json().getAsJsonArray();
        assertEquals("N1 full", resolved.get(0).getAsJsonObject().getAsJsonArray("grebi:name").get(0).getAsString());
        assertEquals(0.75, resolved.get(0).getAsJsonObject().get("grebi:searchScore").getAsDouble(), 1e-9);

        // a hit the resolver does not know keeps its name from the vector index
        when(app.pgClient.resolveToMap("g1", List.of("n1"))).thenReturn(Map.of());
        var unresolved = app.get("/api/v1/graphs/g1/semantic_search?q=psoriasis&model=m1&n=5&resolve=true").json().getAsJsonArray();
        assertEquals("N1", unresolved.get(0).getAsJsonObject().getAsJsonArray("grebi:name").get(0).getAsString());

        // n beyond the vector limit is refused
        assertEquals(400, app.get("/api/v1/graphs/g1/semantic_search?q=psoriasis&model=m1&n=500").status());
    }

    @Test
    void semanticSearchCanBePagedAndFacetedLikeTheLexicalOne() throws Exception {
        var client = mock(EmbeddingServiceClient.class);
        when(client.getAvailableModels()).thenReturn(List.of("m1"));
        when(client.getEmbeddableModels()).thenReturn(Set.of("m1"));
        when(client.embedText("m1", "psoriasis")).thenReturn(new float[] {0.1f});
        app.embeddingClients.put("g1", client);
        var hits = List.of(
            new GrebiPostgresClient.VectorSearchResult("n1", "N1", List.of("GWAS"), List.of("biolink:Disease"), List.of(), 0.1),
            new GrebiPostgresClient.VectorSearchResult("n2", "N2", List.of("OLS.mondo"), List.of("biolink:Disease"), List.of(), 0.2),
            new GrebiPostgresClient.VectorSearchResult("n3", "N3", List.of("GWAS"), List.of("biolink:Gene"), List.of(), 0.3));
        // the candidates are the nearest up to the vector limit, among those matching the filters
        when(app.postgres.searchByVector(eq("g1"), eq("m1"), any(), eq(ResourceLimits.DEFAULT_MAX_VECTOR_RESULTS), eq(Map.of("grebi:datasources", List.of("GWAS", "OLS.mondo")))))
            .thenReturn(hits);

        var first = app.get("/api/v1/graphs/g1/semantic_search?q=psoriasis&model=m1&page=0&size=2&resolve=false&grebi:datasources=GWAS&grebi:datasources=OLS.mondo").json().getAsJsonObject();
        assertEquals(3, first.get("totalElements").getAsInt(), "every candidate counts, not just the page");
        assertEquals(2, first.get("totalPages").getAsInt());
        var content = first.getAsJsonArray("content");
        assertEquals(2, content.size());
        assertEquals("n1", content.get(0).getAsJsonObject().get("grebi:nodeId").getAsString());
        assertEquals(0.9, content.get(0).getAsJsonObject().get("grebi:searchScore").getAsDouble(), 1e-9);
        assertEquals("N1", content.get(0).getAsJsonObject().getAsJsonArray("grebi:name").get(0).getAsString());
        var facets = first.getAsJsonObject("facetFieldToCounts");
        assertEquals(2, facets.getAsJsonObject("grebi:type").get("biolink:Disease").getAsInt());
        assertEquals(1, facets.getAsJsonObject("grebi:type").get("biolink:Gene").getAsInt());
        assertEquals(2, facets.getAsJsonObject("grebi:datasources").get("GWAS").getAsInt());

        var second = app.get("/api/v1/graphs/g1/semantic_search?q=psoriasis&model=m1&page=1&size=2&resolve=false&grebi:datasources=GWAS&grebi:datasources=OLS.mondo").json().getAsJsonObject();
        assertEquals(1, second.getAsJsonArray("content").size());
        assertEquals("n3", second.getAsJsonArray("content").get(0).getAsJsonObject().get("grebi:nodeId").getAsString());

        // resolved hits come from the blobs, localised
        var full = new HashMap<String, Object>(Map.of("grebi:nodeId", "n1", "grebi:name", List.of("N1 full")));
        when(app.pgClient.resolveToMap("g1", List.of("n1", "n2"))).thenReturn(Map.of("n1", full));
        var resolved = app.get("/api/v1/graphs/g1/semantic_search?q=psoriasis&model=m1&page=0&size=2&resolve=true&grebi:datasources=GWAS&grebi:datasources=OLS.mondo").json().getAsJsonObject();
        assertEquals("N1 full", resolved.getAsJsonArray("content").get(0).getAsJsonObject().getAsJsonArray("grebi:name").get(0).getAsString());
        assertEquals("N2", resolved.getAsJsonArray("content").get(1).getAsJsonObject().getAsJsonArray("grebi:name").get(0).getAsString(), "unresolved hits keep the index's name");
    }

    @Test
    void similarNodesUseTheNodesOwnEmbedding() {
        var client = mock(EmbeddingServiceClient.class);
        when(client.getAvailableModels()).thenReturn(List.of("m1"));
        app.embeddingClients.put("g1", client);
        when(app.postgres.getNodeEmbedding("g1", "n1", "m1")).thenReturn(new float[] {0.5f});
        when(app.postgres.searchByVector(eq("g1"), eq("m1"), any(), eq(3)))
            .thenReturn(List.of(new GrebiPostgresClient.VectorSearchResult("n2", "N2", List.of(), List.of(), List.of(), 0.1)));

        var res = app.get("/api/v1/graphs/g1/nodes/" + TestApp.encodeId("n1") + "/similar?n=3");
        assertEquals(200, res.status());
        assertEquals("n2", res.json().getAsJsonArray().get(0).getAsJsonObject().get("nodeId").getAsString());
        verify(app.postgres).getNodeEmbedding("g1", "n1", "m1");

        var noEmbedding = app.get("/api/v1/graphs/g1/nodes/" + TestApp.encodeId("n9") + "/similar?model=m1");
        assertEquals(404, noEmbedding.status());
        assertEquals("No embedding found for this node and model", noEmbedding.json().getAsJsonObject().get("error").getAsString());
    }
}
