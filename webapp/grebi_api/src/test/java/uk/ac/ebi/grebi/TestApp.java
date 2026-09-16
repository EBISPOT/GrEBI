package uk.ac.ebi.grebi;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.javalin.Javalin;
import uk.ac.ebi.grebi.db.EmbeddingServiceClient;
import uk.ac.ebi.grebi.db.GrebiPostgresClient;
import uk.ac.ebi.grebi.repo.GrebiCypherRepo;
import uk.ac.ebi.grebi.repo.GrebiMetadataRepo;
import uk.ac.ebi.grebi.repo.GrebiPostgresRepo;
import uk.ac.ebi.grebi.repo.GrebiQueryTemplatesRepo;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import org.springframework.data.domain.PageRequest;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The API started on a random localhost port over mocked repositories, with the
 * query templates read from src/test/resources/query_templates. Every test gets
 * its own instance, so stubbing never leaks between tests.
 */
final class TestApp implements AutoCloseable {

    static final Gson GSON = new Gson();
    static final String FIXTURES = "src/test/resources/query_templates";
    static final Set<String> GRAPHS = new LinkedHashSet<>(List.of("g1", "g2"));

    /** null when the app is started without a cypher service */
    final GrebiCypherRepo cypher;
    final GrebiPostgresRepo postgres;
    final GrebiPostgresClient pgClient;
    final GrebiMetadataRepo metadata;
    final GrebiQueryTemplatesRepo templates;
    final Map<String, EmbeddingServiceClient> embeddingClients = new LinkedHashMap<>();
    final Javalin app;
    final String baseUrl;
    private final HttpClient http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();

    static TestApp start() {
        return new TestApp(true);
    }

    static TestApp startWithoutCypher() {
        return new TestApp(false);
    }

    private TestApp(boolean withCypher) {
        cypher = withCypher ? mock(GrebiCypherRepo.class) : null;
        if (cypher != null) {
            when(cypher.getStats()).thenReturn(Map.of("g1", Map.of("num_nodes", 42L, "num_edges", 7L)));
        }
        pgClient = mock(GrebiPostgresClient.class);
        postgres = mock(GrebiPostgresRepo.class);
        when(postgres.getPgClient()).thenReturn(pgClient);
        metadata = mock(GrebiMetadataRepo.class);
        when(metadata.getGraphs()).thenReturn(GRAPHS);
        // a fresh copy per call: some routes annotate the elements they return
        when(metadata.getMetadata("g1")).thenAnswer(inv -> graphMetadata("g1"));
        when(metadata.getMetadata("g2")).thenAnswer(inv -> graphMetadata("g2"));
        templates = new GrebiQueryTemplatesRepo(FIXTURES);
        app = GrebiApi.createApp(cypher, postgres, metadata, GRAPHS, templates, embeddingClients)
            .start("127.0.0.1", 0);
        baseUrl = "http://127.0.0.1:" + app.port();
    }

    @Override
    public void close() {
        app.stop();
    }

    /** The graph_metadata a dataload records, as the metadata repository serves it. */
    static Map<String, JsonElement> graphMetadata(String graph) {
        String json = switch (graph) {
            case "g1" -> """
                {
                  "subgraph_name": "g1",
                  "types": {"biolink:Disease": {"count": 10}, "gwas:Study": {"count": 5}, "not-an-object": 3},
                  "node_counts_by_datasource": {"GWAS": 5, "OLS.efo": 10},
                  "edges": {
                    "gwas:SNP": {"gwas:associated_with": {"biolink:Disease": {"GWAS": 7, "GWAS,OLS.efo": 3}}},
                    "biolink:Disease": {"biolink:subclass_of": {"biolink:Disease": {"OLS.efo": 20}}}
                  },
                  "entity_props": {"grebi:name": {}},
                  "entity_prop_defs": {},
                  "edge_props": {"grebi:type": {}, "gwas:p_value": {}},
                  "edge_prop_defs": {},
                  "embedding_pca_models": {"text-embedding-3-small": {"dims": 3}},
                  "materialised_queries": [
                    {"id": "all_studies", "table": "matq_g1_all_studies", "mode": "full",
                     "columns": [{"column_id": "study", "column_type": "GraphNodeId", "facet": true}]}
                  ],
                  "materialised_templates": [
                    {"id": "snps_by_trait_materialised", "table": "matq_g1_snps_by_trait", "mode": "full", "closure_key": "nid",
                     "columns": [{"column_id": "trait", "column_type": "GraphNodeId", "facet": true},
                                 {"column_id": "snp", "column_type": "GraphNodeId"},
                                 {"column_id": "p_value", "column_type": "float"}],
                     "params": [{"param_id": "trait_id", "filters_column": "trait", "closure": "descendants", "param_type": "SourceId"}]},
                    {"id": "study_counts_by_trait", "table": "matq_g1_study_counts", "mode": "counts_only",
                     "columns": [{"column_id": "study", "column_type": "GraphNodeId"}],
                     "params": [{"param_id": "trait_id", "filters_column": "study", "closure": "exact", "param_type": "SourceId"}]}
                  ]
                }
                """;
            case "g2" -> """
                {"subgraph_name": "g2", "types": {}, "node_counts_by_datasource": {}, "edges": {},
                 "entity_props": {}, "edge_props": {}}
                """;
            default -> throw new IllegalArgumentException(graph);
        };
        return JsonParser.parseString(json).getAsJsonObject().asMap();
    }

    /** A page of rows with facet counts, as the Postgres repository returns them. */
    static GrebiFacetedResultsPage<Map<String, Object>> facetedPage(
            List<Map<String, Object>> rows, Map<String, Map<String, Long>> facets, long total) {
        return new GrebiFacetedResultsPage<>(rows, facets, PageRequest.of(0, 10), total);
    }

    /** A mutable row: some routes add keys to what the repository returned. */
    static Map<String, Object> row(Object... keysAndValues) {
        var row = new LinkedHashMap<String, Object>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            row.put((String) keysAndValues[i], keysAndValues[i + 1]);
        }
        return row;
    }

    static String encodeId(String id) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(id.getBytes(StandardCharsets.UTF_8));
    }

    record Response(int status, String body, HttpHeaders headers) {
        JsonElement json() {
            return JsonParser.parseString(body);
        }
        String header(String name) {
            return headers.firstValue(name).orElse(null);
        }
    }

    Response get(String path) {
        return send(HttpRequest.newBuilder(URI.create(baseUrl + path)).GET(), Map.of());
    }

    Response get(String path, Map<String, String> headers) {
        return send(HttpRequest.newBuilder(URI.create(baseUrl + path)).GET(), headers);
    }

    Response post(String path, String body) {
        return send(HttpRequest.newBuilder(URI.create(baseUrl + path))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body)), Map.of());
    }

    private Response send(HttpRequest.Builder request, Map<String, String> headers) {
        headers.forEach(request::header);
        try {
            var res = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
            return new Response(res.statusCode(), res.body(), res.headers());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
