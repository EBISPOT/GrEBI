package uk.ac.ebi.grebi;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpError;
import io.modelcontextprotocol.spec.McpSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * The MCP server, driven over its streamable-HTTP endpoint by the MCP SDK's own
 * client, so the real protocol is exercised: initialize, tool listing and calls,
 * resources.
 */
class GrebiMcpServerTest {

    TestApp app;
    McpSyncClient client;

    @BeforeEach
    void start() {
        app = TestApp.start();
        client = connect(app);
    }

    @AfterEach
    void stop() {
        client.close();
        app.close();
    }

    static McpSyncClient connect(TestApp app) {
        var transport = HttpClientStreamableHttpTransport.builder(app.baseUrl).endpoint("/api/v1/mcp").build();
        var c = McpClient.sync(transport).requestTimeout(Duration.ofSeconds(30)).build();
        c.initialize();
        return c;
    }

    static JsonObject text(McpSchema.CallToolResult result) {
        return JsonParser.parseString(((McpSchema.TextContent) result.content().get(0)).text()).getAsJsonObject();
    }

    static McpSchema.CallToolRequest call(String tool, Map<String, Object> args) {
        return new McpSchema.CallToolRequest(tool, args);
    }

    /** The message a failing tool call comes back with, however the SDK surfaces it. */
    static String toolError(McpSyncClient client, McpSchema.CallToolRequest request) {
        try {
            var result = client.callTool(request);
            assertTrue(Boolean.TRUE.equals(result.isError()), "expected a tool error, got " + result);
            return ((McpSchema.TextContent) result.content().get(0)).text();
        } catch (McpError e) {
            return e.getMessage();
        }
    }

    @Test
    void initializeIdentifiesTheServerAndItsInstructions() {
        var info = client.getServerInfo();
        assertEquals("grebi", info.name());
        assertEquals("1.0.0", info.version());
        assertTrue(client.getServerInstructions().contains("query templates"));
        assertNotNull(client.getServerCapabilities().tools());
        assertNotNull(client.getServerCapabilities().resources());
    }

    @Test
    void thereIsAToolPerParameterisedTemplatePlusTheFixedOnes() {
        var tools = client.listTools().tools().stream().collect(Collectors.toMap(McpSchema.Tool::name, t -> t));
        assertTrue(tools.keySet().containsAll(Set.of("search_nodes", "get_node", "get_node_edge_counts", "list_node_edges", "get_edge",
            "studies_by_trait", "snps_by_trait_materialised", "study_counts_by_trait", "node_count")), tools.keySet().toString());
        assertFalse(tools.containsKey("all_studies"), "a standalone materialised query is a table, not a tool");

        var studies = tools.get("studies_by_trait");
        assertEquals("GWAS studies annotated with a trait: Studies whose mapped trait is the given term.", studies.description());
        assertEquals(List.of("graph", "trait_id", "sortBy", "sortDir", "pageNum", "pageSize"), List.copyOf(studies.inputSchema().properties().keySet()));
        assertEquals(List.of("g1"), ((Map<?, ?>) studies.inputSchema().properties().get("graph")).get("enum"));
        assertEquals(List.of("trait", "study", "study_accession", "p_value"),
            ((Map<?, ?>) studies.inputSchema().properties().get("sortBy")).get("enum"), "the edge id column is not sortable");
        var rows = (Map<?, ?>) ((Map<?, ?>) ((Map<?, ?>) studies.outputSchema().get("properties")).get("rows")).get("items");
        var rowProps = (Map<?, ?>) rows.get("properties");
        assertEquals(Map.of("type", "string"), rowProps.get("study_accession"));
        assertEquals(Map.of("type", List.of("number", "null")), rowProps.get("p_value"), "optional columns are nullable");
        assertEquals(Map.of("type", "object"), rowProps.get("study"));
        assertFalse(rowProps.containsKey("edge_id"), "edge ids are internal");

        // a template without a graphs list is offered on every graph, and one without params has only the controls
        var count = tools.get("node_count");
        assertEquals(List.of("g1", "g2"), ((Map<?, ?>) count.inputSchema().properties().get("graph")).get("enum"));
        assertEquals(List.of("graph", "sortBy", "sortDir", "pageNum", "pageSize"), List.copyOf(count.inputSchema().properties().keySet()));

        var search = tools.get("search_nodes");
        assertEquals(List.of("graph"), search.inputSchema().required());
        assertEquals(List.of("g1", "g2"), ((Map<?, ?>) search.inputSchema().properties().get("graph")).get("enum"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void searchNodesPagesThroughPostgresAndReportsFacets() {
        when(app.postgres.searchNodesPaginated(eq("g1"), eq("psoriasis"), any(), anyBoolean(), any()))
            .thenReturn(TestApp.facetedPage(List.of(TestApp.row("grebi:nodeId", "mondo:0005083")), Map.of("grebi:type", Map.of("biolink:Disease", 1L)), 1));

        var result = client.callTool(call("search_nodes", Map.of("graph", "g1", "q", "psoriasis",
            "filters", Map.of("grebi:type", List.of("a", "b"), "grebi:datasources", "GWAS"), "pageNum", 0, "pageSize", 5, "resolve", false)));
        assertNotEquals(Boolean.TRUE, result.isError());
        var body = text(result);
        assertEquals("mondo:0005083", body.getAsJsonArray("rows").get(0).getAsJsonObject().get("grebi:nodeId").getAsString());
        assertEquals(1, body.get("totalNumRows").getAsInt());
        assertEquals(1, body.getAsJsonObject("facets").getAsJsonObject("grebi:type").get("biolink:Disease").getAsInt());
        assertEquals(1, ((Number) result.structuredContent().get("totalNumRows")).intValue());

        var filters = ArgumentCaptor.forClass(Map.class);
        var page = ArgumentCaptor.forClass(Pageable.class);
        verify(app.postgres).searchNodesPaginated(eq("g1"), eq("psoriasis"), filters.capture(), eq(false), page.capture());
        assertEquals(Map.of("grebi:type", List.of("a", "b"), "grebi:datasources", List.of("GWAS")), filters.getValue(),
            "a filter value may be a string or a list of strings");
        assertEquals(5, page.getValue().getPageSize());

        assertTrue(toolError(client, call("search_nodes", Map.of("graph", "nope"))).contains("Unknown graph nope"));
        assertTrue(toolError(client, call("search_nodes", Map.of())).contains("Missing required argument: graph"));
        assertTrue(toolError(client, call("search_nodes", Map.of("graph", "g1", "filters", "not an object"))).contains("filters must be an object"));
    }

    @Test
    void getNodeResolvesAndReportsMissingNodes() {
        when(app.pgClient.resolveToList("g1", List.of("mondo:0005083"))).thenReturn(List.of(TestApp.row("grebi:nodeId", "mondo:0005083", "grebi:name", List.of("psoriasis"))));
        var result = client.callTool(call("get_node", Map.of("graph", "g1", "nodeId", "mondo:0005083")));
        assertEquals("psoriasis", text(result).getAsJsonObject("node").getAsJsonArray("grebi:name").get(0).getAsString());
        assertTrue(result.structuredContent().containsKey("node"));

        assertTrue(toolError(client, call("get_node", Map.of("graph", "g1", "nodeId", "mondo:0000000"))).contains("Node not found"));
        assertTrue(toolError(client, call("get_node", Map.of("graph", "g1"))).contains("Missing required argument: nodeId"));
    }

    @Test
    void nodesCanBeAskedForInALanguage() {
        when(app.pgClient.resolveToList("g1", List.of("mondo:0005083"))).thenReturn(List.of(GrebiApiNodeRoutesTest.translatedNode()));
        var node = text(client.callTool(call("get_node", Map.of("graph", "g1", "nodeId", "mondo:0005083", "lang", "fr")))).getAsJsonObject("node");
        var names = node.getAsJsonArray("grebi:name");
        assertEquals(2, names.size());
        assertEquals("psoriasis (fr)", names.get(0).getAsJsonObject().getAsJsonObject("grebi:value").get("grebi:value").getAsString());
        assertEquals(List.of("en", "de", "fr", "ja"), TestApp.GSON.fromJson(node.get("grebi:languages"), List.class));

        when(app.postgres.searchNodesPaginated(eq("g1"), eq("psoriasis"), any(), eq(true), any()))
            .thenReturn(TestApp.facetedPage(List.of(GrebiApiNodeRoutesTest.translatedNode()), Map.of(), 1));
        var hit = text(client.callTool(call("search_nodes", Map.of("graph", "g1", "q", "psoriasis", "lang", "de")))).getAsJsonArray("rows").get(0).getAsJsonObject();
        assertEquals("Schuppenflechte", hit.getAsJsonArray("grebi:synonym").get(0).getAsJsonObject().getAsJsonObject("grebi:value").get("grebi:value").getAsString());
        assertEquals(1, hit.getAsJsonArray("grebi:name").size(), "no German name, so English alone");

        var tools = client.listTools().tools().stream().collect(Collectors.toMap(McpSchema.Tool::name, t -> t));
        assertTrue(tools.get("get_node").inputSchema().properties().containsKey("lang"));
        assertTrue(tools.get("search_nodes").inputSchema().properties().containsKey("lang"));
    }

    @Test
    void edgeCountsFollowTheDirection() {
        when(app.postgres.getBothEdgeCounts("g1", "n1")).thenReturn(Map.of("incoming", Map.of("t", Map.of("ds", 1))));
        when(app.postgres.getIncomingEdgeCounts("g1", "n1")).thenReturn(Map.of("t", Map.of("ds", 2)));
        when(app.postgres.getOutgoingEdgeCounts("g1", "n1")).thenReturn(Map.of("t", Map.of("ds", 3)));

        assertEquals(1, text(client.callTool(call("get_node_edge_counts", Map.of("graph", "g1", "nodeId", "n1"))))
            .getAsJsonObject("counts").getAsJsonObject("incoming").getAsJsonObject("t").get("ds").getAsInt(), "both by default");
        assertEquals(2, text(client.callTool(call("get_node_edge_counts", Map.of("graph", "g1", "nodeId", "n1", "direction", "incoming"))))
            .getAsJsonObject("counts").getAsJsonObject("t").get("ds").getAsInt());
        assertEquals(3, text(client.callTool(call("get_node_edge_counts", Map.of("graph", "g1", "nodeId", "n1", "direction", "outgoing"))))
            .getAsJsonObject("counts").getAsJsonObject("t").get("ds").getAsInt());
        assertTrue(toolError(client, call("get_node_edge_counts", Map.of("graph", "g1", "nodeId", "n1", "direction", "sideways"))).contains("Unknown direction sideways"));
    }

    @Test
    void listNodeEdgesMapsTheDirectionToTheEdgeSide() {
        var page = TestApp.facetedPage(List.of(TestApp.row("grebi:edgeId", "e1")), Map.of(), 1);
        when(app.postgres.searchEdgesPaginated(eq("g1"), anyString(), eq("n1"), any(), anyString(), anyString(), any())).thenReturn(page);
        when(app.postgres.searchEdgeRefsPaginated(eq("g1"), anyString(), eq("n1"), any(), anyString(), anyString(), any())).thenReturn(page);

        var incoming = client.callTool(call("list_node_edges", Map.of("graph", "g1", "nodeId", "n1", "direction", "incoming",
            "filters", Map.of("grebi:type", "biolink:subclass_of"), "sortBy", "grebi:datasources", "sortDir", "desc", "pageSize", 3)));
        assertEquals("e1", text(incoming).getAsJsonArray("rows").get(0).getAsJsonObject().get("grebi:edgeId").getAsString());
        verify(app.postgres).searchEdgesPaginated(eq("g1"), eq("grebi:toNodeId"), eq("n1"), eq(Map.of("grebi:type", List.of("biolink:subclass_of"))),
            eq("grebi:datasources"), eq("desc"), argThat(p -> p.getPageSize() == 3));

        client.callTool(call("list_node_edges", Map.of("graph", "g1", "nodeId", "n1", "direction", "outgoing", "refsOnly", true)));
        verify(app.postgres).searchEdgeRefsPaginated(eq("g1"), eq("grebi:fromNodeId"), eq("n1"), eq(Map.of()), eq("grebi:type"), eq("asc"), any());

        assertTrue(toolError(client, call("list_node_edges", Map.of("graph", "g1", "nodeId", "n1", "direction", "both"))).contains("Unknown direction both"));
        assertTrue(toolError(client, call("list_node_edges", Map.of("graph", "g1", "nodeId", "n1", "direction", "incoming", "sortDir", "up"))).contains("Unknown sort direction up"));
    }

    @Test
    void getEdgeStripsTheGraphPrefixAndAttachesTheEnds() {
        var edge = TestApp.row("grebi:edgeId", "e1", "grebi:fromNodeId", "a", "grebi:toNodeId", "b",
            "_refs", Map.of("a", Map.of("grebi:name", List.of("A")), "b", Map.of("grebi:name", List.of("B"))));
        when(app.pgClient.resolveToMap("g1", List.of("e1"))).thenReturn(Map.of("e1", edge));

        var result = text(client.callTool(call("get_edge", Map.of("graph", "g1", "edgeId", "g1:e1")))).getAsJsonObject("edge");
        assertEquals("A", result.getAsJsonObject("from").getAsJsonArray("grebi:name").get(0).getAsString());
        assertEquals("B", result.getAsJsonObject("to").getAsJsonArray("grebi:name").get(0).getAsString());
        assertTrue(toolError(client, call("get_edge", Map.of("graph", "g1", "edgeId", "nope"))).contains("Edge not found"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void aTemplateToolRunsTheQueryAndDropsTheEdgeIdColumns() {
        when(app.cypher.runQueryFromTemplatePaginated(eq("g1"), any(), any(), anyBoolean(), any()))
            .thenReturn(new PageImpl<>(List.of(TestApp.row("study", Map.of("id", "GCST1"), "study_accession", "GCST1", "edge_id", "e1")), PageRequest.of(0, 10), 1));

        var result = client.callTool(call("studies_by_trait", Map.of("graph", "g1", "trait_id", "mondo:0005083",
            "sortBy", "study_accession", "sortDir", "desc", "pageNum", 0, "pageSize", 10)));
        var body = text(result);
        var row = body.getAsJsonArray("rows").get(0).getAsJsonObject();
        assertEquals("GCST1", row.get("study_accession").getAsString());
        assertFalse(row.has("edge_id"), "edge ids are internal");
        assertEquals(1, body.get("totalNumRows").getAsInt());
        assertEquals(10, body.get("pageSize").getAsInt());

        var params = ArgumentCaptor.forClass(Map.class);
        verify(app.cypher).runQueryFromTemplatePaginated(eq("g1"), argThat(t -> t.id.equals("studies_by_trait")), params.capture(), eq(false),
            argThat(p -> p.getSort().getOrderFor("study_accession").isDescending()));
        assertEquals(Map.of("trait_id", List.of("mondo:0005083")), params.getValue());

        // g2 is a graph the server has, but not one this template supports
        assertTrue(toolError(client, call("studies_by_trait", Map.of("graph", "g2", "trait_id", "x", "sortBy", "study", "sortDir", "asc"))).contains("Unknown graph g2"));
        assertTrue(toolError(client, call("studies_by_trait", Map.of("graph", "g1", "trait_id", "x", "sortBy", "study", "sortDir", "sideways"))).contains("Unknown sort direction"));
        assertTrue(toolError(client, call("studies_by_trait", Map.of("graph", "g1", "trait_id", "x", "sortBy", "nope", "sortDir", "asc"))).contains("Unknown sort column nope"));
    }

    @Test
    void aMaterialisedTemplateToolIsServedFromPostgres() {
        when(app.postgres.runMaterialisedParameterisedPaginated(eq("g1"), any(), any(), any(), isNull(), any(), anyBoolean(), any()))
            .thenReturn(TestApp.facetedPage(List.of(TestApp.row("snp", Map.of("id", "rs1"))), Map.of(), 1));

        var body = text(client.callTool(call("snps_by_trait_materialised", Map.of("graph", "g1", "trait_id", "efo:1", "sortBy", "snp", "sortDir", "asc", "pageNum", 0, "pageSize", 10))));
        assertEquals("rs1", body.getAsJsonArray("rows").get(0).getAsJsonObject().getAsJsonObject("snp").get("id").getAsString());
        verify(app.postgres).runMaterialisedParameterisedPaginated(eq("g1"), argThat(t -> t.id.equals("snps_by_trait_materialised")),
            argThat(b -> "matq_g1_snps_by_trait".equals(b.table)), eq(Map.of("trait_id", List.of("efo:1"))), isNull(), eq(Map.of()), eq(false), any());
        verify(app.cypher, never()).runQueryFromTemplatePaginated(any(), any(), any(), anyBoolean(), any());
    }

    @Test
    void resourcesExposeStatsTopicsGraphsAndTemplates() {
        var uris = client.listResources().resources().stream().map(McpSchema.Resource::uri).collect(Collectors.toSet());
        assertEquals(Set.of("grebi://stats", "grebi://topics", "grebi://graphs", "grebi://query_templates"), uris);

        assertEquals("[\"g1\",\"g2\"]", read("grebi://graphs"));
        assertEquals(42, JsonParser.parseString(read("grebi://stats")).getAsJsonObject().getAsJsonObject("g1").get("num_nodes").getAsInt());
        assertEquals(2, JsonParser.parseString(read("grebi://topics")).getAsJsonArray().size());
        var templates = JsonParser.parseString(read("grebi://query_templates")).getAsJsonArray();
        assertEquals(5, templates.size(), "every template, standalone queries included");
    }

    @Test
    void withoutTheCypherServiceTheStatsResourceIsNull() {
        try (var noCypher = TestApp.startWithoutCypher()) {
            var c = connect(noCypher);
            try {
                assertEquals("null", ((McpSchema.TextResourceContents) c.readResource(new McpSchema.ReadResourceRequest("grebi://stats")).contents().get(0)).text());
            } finally {
                c.close();
            }
        }
    }

    @Test
    void anUnknownToolIsAnError() {
        assertThrows(McpError.class, () -> client.callTool(call("nope", Map.of())));
    }

    String read(String uri) {
        var contents = client.readResource(new McpSchema.ReadResourceRequest(uri)).contents().get(0);
        assertEquals("application/json", ((McpSchema.TextResourceContents) contents).mimeType());
        return ((McpSchema.TextResourceContents) contents).text();
    }
}
