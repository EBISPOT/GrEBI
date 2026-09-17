package uk.ac.ebi.grebi;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import uk.ac.ebi.grebi.repo.GrebiCypherRepo;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Node search, node pages, edge counts and the per-node edge lists. */
class GrebiApiNodeRoutesTest {

    static final String NODE = "mondo:0005083";
    static final String ENC = TestApp.encodeId(NODE);

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
    void nodeListingPassesFiltersResolveAndPageToPostgres() {
        when(app.postgres.searchNodesPaginated(eq("g1"), isNull(), any(), anyBoolean(), any()))
            .thenReturn(TestApp.facetedPage(List.of(TestApp.row("grebi:nodeId", NODE)), Map.of(), 1));

        var res = app.get("/api/v1/graphs/g1/nodes?grebi:type=biolink:Disease&grebi:sourceIds=" + NODE + "&page=1&size=7&facet=x");
        assertEquals(200, res.status());
        assertEquals(1, res.json().getAsJsonObject().get("totalElements").getAsInt());

        var filters = ArgumentCaptor.forClass(Map.class);
        var page = ArgumentCaptor.forClass(Pageable.class);
        verify(app.postgres).searchNodesPaginated(eq("g1"), isNull(), filters.capture(), eq(true), page.capture());
        assertEquals(Map.of("grebi:type", List.of("biolink:Disease"), "grebi:sourceIds", List.of(NODE)), filters.getValue(),
            "page, size and facet are not filters");
        assertEquals(1, page.getValue().getPageNumber());
        assertEquals(7, page.getValue().getPageSize());

        app.get("/api/v1/graphs/g1/nodes?resolve=false");
        verify(app.postgres).searchNodesPaginated(eq("g1"), isNull(), eq(Map.of()), eq(false), any());
    }

    @Test
    void aNodeIsLookedUpByItsBase64UrlEncodedId() {
        when(app.pgClient.resolveToList("g1", List.of(NODE))).thenReturn(List.of(TestApp.row("grebi:nodeId", NODE, "grebi:name", List.of("psoriasis"))));

        var res = app.get("/api/v1/graphs/g1/nodes/" + ENC);
        assertEquals(200, res.status());
        assertEquals("psoriasis", res.json().getAsJsonObject().getAsJsonArray("grebi:name").get(0).getAsString());

        var missing = app.get("/api/v1/graphs/g1/nodes/" + TestApp.encodeId("mondo:0000000"));
        assertEquals(404, missing.status());
        assertEquals("Node not found", missing.json().getAsJsonObject().get("error").getAsString());
    }

    /** A node as the dataload merges it, with its name in three languages and a synonym in a fourth. */
    static Map<String, Object> translatedNode() {
        return TestApp.row("grebi:nodeId", NODE,
            "grebi:name", List.of(LanguagesTest.merged("psoriasis"), LanguagesTest.merged(LanguagesTest.translated("psoriasis (fr)", "fr")),
                LanguagesTest.merged(LanguagesTest.translated("乾癬", "ja"))),
            "grebi:synonym", List.of(LanguagesTest.merged(LanguagesTest.translated("Schuppenflechte", "de"))));
    }

    @Test
    void aNodeIsServedInTheLanguageAskedFor() {
        when(app.pgClient.resolveToList("g1", List.of(NODE))).thenReturn(List.of(translatedNode()));

        var all = app.get("/api/v1/graphs/g1/nodes/" + ENC).json().getAsJsonObject();
        assertEquals(3, all.getAsJsonArray("grebi:name").size(), "without a language every value is served");
        assertEquals(List.of("en", "de", "fr", "ja"), TestApp.GSON.fromJson(all.get("grebi:languages"), List.class));

        var french = app.get("/api/v1/graphs/g1/nodes/" + ENC + "?lang=fr").json().getAsJsonObject();
        var names = french.getAsJsonArray("grebi:name");
        assertEquals(2, names.size());
        assertEquals("psoriasis (fr)", names.get(0).getAsJsonObject().getAsJsonObject("grebi:value").get("grebi:value").getAsString(), "the language asked for first");
        assertEquals("psoriasis", names.get(1).getAsJsonObject().get("grebi:value").getAsString(), "then English as the fallback");
        assertFalse(french.has("grebi:synonym"), "a property with values only in other languages is dropped");

        var english = app.get("/api/v1/graphs/g1/nodes/" + ENC + "?lang=en").json().getAsJsonObject();
        assertEquals(1, english.getAsJsonArray("grebi:name").size());
        assertEquals(List.of("en", "de", "fr", "ja"), TestApp.GSON.fromJson(english.get("grebi:languages"), List.class), "still listed");
    }

    @Test
    void resolvedSearchResultsAreServedInTheLanguageAskedFor() {
        when(app.postgres.searchNodesPaginated(eq("g1"), eq("psoriasis"), any(), eq(true), any()))
            .thenReturn(TestApp.facetedPage(List.of(translatedNode()), Map.of(), 1));
        when(app.postgres.searchNodesPaginated(eq("g1"), eq("psoriasis"), any(), eq(false), any()))
            .thenReturn(TestApp.facetedPage(List.of(TestApp.row("grebi:nodeId", NODE, "grebi:name", "psoriasis")), Map.of(), 1));

        var hit = app.get("/api/v1/graphs/g1/search?q=psoriasis&lang=ja").json().getAsJsonObject().getAsJsonArray("content").get(0).getAsJsonObject();
        assertEquals("乾癬", hit.getAsJsonArray("grebi:name").get(0).getAsJsonObject().getAsJsonObject("grebi:value").get("grebi:value").getAsString());
        assertEquals(2, hit.getAsJsonArray("grebi:name").size());

        var light = app.get("/api/v1/graphs/g1/search?q=psoriasis&lang=ja&resolve=false").json().getAsJsonObject().getAsJsonArray("content").get(0).getAsJsonObject();
        assertEquals("psoriasis", light.get("grebi:name").getAsString(), "unresolved hits carry the name column, which is English");
        assertFalse(light.has("grebi:languages"));
    }

    @Test
    void edgeCountsComeFromPostgresAndAreCacheable() {
        when(app.postgres.getBothEdgeCounts("g1", NODE)).thenReturn(Map.of("incoming", Map.of("biolink:subclass_of", Map.of("OLS.efo", 3))));
        when(app.postgres.getIncomingEdgeCounts("g1", NODE)).thenReturn(Map.of("biolink:subclass_of", Map.of("OLS.efo", 3)));
        when(app.postgres.getOutgoingEdgeCounts("g1", NODE)).thenReturn(Map.of("biolink:broad_match", Map.of("OLS.efo", 2)));

        var both = app.get("/api/v1/graphs/g1/nodes/" + ENC + "/edge_counts");
        assertEquals("public, max-age=600", both.header("cache-control"));
        assertEquals(3, both.json().getAsJsonObject().getAsJsonObject("incoming").getAsJsonObject("biolink:subclass_of").get("OLS.efo").getAsInt());
        assertEquals(3, app.get("/api/v1/graphs/g1/nodes/" + ENC + "/incoming_edge_counts").json().getAsJsonObject()
            .getAsJsonObject("biolink:subclass_of").get("OLS.efo").getAsInt());
        assertEquals(2, app.get("/api/v1/graphs/g1/nodes/" + ENC + "/outgoing_edge_counts").json().getAsJsonObject()
            .getAsJsonObject("biolink:broad_match").get("OLS.efo").getAsInt());
    }

    @Test
    @SuppressWarnings("unchecked")
    void resolvingSingleEdgesNeedsTheCypherServiceAndABody() {
        var path = "/api/v1/graphs/g1/nodes/" + ENC + "/resolve_single_edges";
        try (var noCypher = TestApp.startWithoutCypher()) {
            assertEquals("{}", noCypher.post(path, "[{\"direction\":\"incoming\",\"edgeType\":\"biolink:subclass_of\"}]").body());
        }
        assertEquals("{}", app.post(path, "[]").body());
        verify(app.cypher, never()).resolveSingleEdges(any(), any(), any());

        when(app.cypher.resolveSingleEdges(eq("g1"), eq(NODE), any()))
            .thenReturn(Map.of("incoming|biolink:subclass_of", Map.of("grebi:nodeId", "efo:1")));
        var res = app.post(path, "[{\"direction\":\"incoming\",\"edgeType\":\"biolink:subclass_of\"}]");
        assertEquals(200, res.status());
        assertEquals("efo:1", res.json().getAsJsonObject().getAsJsonObject("incoming|biolink:subclass_of").get("grebi:nodeId").getAsString());
        var items = ArgumentCaptor.forClass(List.class);
        verify(app.cypher).resolveSingleEdges(eq("g1"), eq(NODE), items.capture());
        var item = (GrebiCypherRepo.DirectionAndEdgeType) items.getValue().get(0);
        assertEquals("incoming", item.direction);
        assertEquals("biolink:subclass_of", item.edgeType);

        // more entries than the limit allows is a 400
        var tooMany = "[" + "{\"direction\":\"incoming\",\"edgeType\":\"x\"},".repeat(101).replaceAll(",$", "") + "]";
        var rejected = app.post(path, tooMany);
        assertEquals(400, rejected.status());
        assertTrue(rejected.json().getAsJsonObject().get("error").getAsString().contains("100"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void edgeListsFilterOnTheRightSideOfTheNodeAndPassTheOtherParameters() {
        var page = TestApp.facetedPage(List.of(TestApp.row("grebi:edgeId", "e1")), Map.of("grebi:type", Map.of("biolink:subclass_of", 1L)), 1);
        when(app.postgres.searchEdgesPaginated(eq("g1"), anyString(), eq(NODE), any(), anyString(), anyString(), any())).thenReturn(page);
        when(app.postgres.searchEdgeRefsPaginated(eq("g1"), anyString(), eq(NODE), any(), anyString(), anyString(), any())).thenReturn(page);

        var res = app.get("/api/v1/graphs/g1/nodes/" + ENC + "/incoming_edges?grebi:datasources=GWAS&sortBy=grebi:type&sortDir=desc&page=0&size=3&facet=x");
        assertEquals(200, res.status());
        var body = res.json().getAsJsonObject();
        assertEquals("e1", body.getAsJsonArray("content").get(0).getAsJsonObject().get("grebi:edgeId").getAsString());
        assertEquals(1, body.getAsJsonObject("facetFieldToCounts").getAsJsonObject("grebi:type").get("biolink:subclass_of").getAsInt());

        var filters = ArgumentCaptor.forClass(Map.class);
        var pageable = ArgumentCaptor.forClass(Pageable.class);
        verify(app.postgres).searchEdgesPaginated(eq("g1"), eq("grebi:toNodeId"), eq(NODE), filters.capture(), eq("grebi:type"), eq("desc"), pageable.capture());
        assertEquals(Map.of("grebi:datasources", List.of("GWAS")), filters.getValue(), "paging, sorting and facet keys are not filters");
        assertEquals(3, pageable.getValue().getPageSize());
        assertEquals(Sort.by(Sort.Direction.DESC, "grebi:type"), pageable.getValue().getSort());

        app.get("/api/v1/graphs/g1/nodes/" + ENC + "/outgoing_edges");
        verify(app.postgres).searchEdgesPaginated(eq("g1"), eq("grebi:fromNodeId"), eq(NODE), eq(Map.of()), eq("grebi:type"), eq("asc"), any());
        app.get("/api/v1/graphs/g1/nodes/" + ENC + "/incoming_edge_refs");
        verify(app.postgres).searchEdgeRefsPaginated(eq("g1"), eq("grebi:toNodeId"), eq(NODE), eq(Map.of()), eq("grebi:type"), eq("asc"), any());
        app.get("/api/v1/graphs/g1/nodes/" + ENC + "/outgoing_edge_refs?sortDir=desc");
        verify(app.postgres).searchEdgeRefsPaginated(eq("g1"), eq("grebi:fromNodeId"), eq(NODE), eq(Map.of()), eq("grebi:type"), eq("desc"), any());
    }

    @Test
    void searchAndSuggest() {
        when(app.postgres.searchNodesPaginated(eq("g1"), eq("psoriasis"), any(), anyBoolean(), any()))
            .thenReturn(TestApp.facetedPage(List.of(TestApp.row("grebi:nodeId", NODE)), Map.of("grebi:type", Map.of("biolink:Disease", 1L)), 1));
        var res = app.get("/api/v1/graphs/g1/search?q=psoriasis&grebi:type=biolink:Disease&size=5");
        assertEquals(200, res.status());
        var body = res.json().getAsJsonObject();
        assertEquals(1, body.get("totalElements").getAsInt());
        assertEquals(1, body.getAsJsonObject("facetFieldToCounts").getAsJsonObject("grebi:type").get("biolink:Disease").getAsInt());
        verify(app.postgres).searchNodesPaginated(eq("g1"), eq("psoriasis"), eq(Map.of("grebi:type", List.of("biolink:Disease"))), eq(true),
            argThat(p -> p.getPageSize() == 5));

        app.get("/api/v1/graphs/g1/search?q=psoriasis&resolve=false");
        verify(app.postgres).searchNodesPaginated(eq("g1"), eq("psoriasis"), eq(Map.of()), eq(false), any());

        when(app.postgres.autocomplete("g1", "pso")).thenReturn(List.of("psoriasis", "psoriatic arthritis"));
        assertEquals(List.of("psoriasis", "psoriatic arthritis"),
            TestApp.GSON.fromJson(app.get("/api/v1/graphs/g1/suggest?q=pso").body(), List.class));
    }
}
