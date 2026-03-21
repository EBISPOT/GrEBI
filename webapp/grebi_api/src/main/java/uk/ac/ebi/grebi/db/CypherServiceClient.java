package uk.ac.ebi.grebi.db;

import com.google.gson.Gson;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;

/**
 * HTTP client for the grebi_cypher_service.
 * Replaces direct Neo4j bolt driver usage.
 */
public class CypherServiceClient {

    private final String baseUrl;
    private final HttpClient httpClient;
    private final Gson gson = new Gson();

    public CypherServiceClient(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.httpClient = HttpClient.newHttpClient();
    }

    public static String getCypherServiceUrl() {
        var url = System.getenv("GREBI_CYPHER_HOST");
        if (url != null) return url;
        return "http://localhost:8085";
    }

    /**
     * GET / — list loaded subgraphs.
     */
    @SuppressWarnings("unchecked")
    public Set<String> getSubgraphs() throws IOException {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/"))
                .GET()
                .build();
        try {
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() >= 400) {
                throw new IOException("Cypher service error (" + resp.statusCode() + "): " + resp.body());
            }
            List<String> list = gson.fromJson(resp.body(), List.class);
            return new LinkedHashSet<>(list);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while listing subgraphs", e);
        }
    }

    /**
     * POST /{subgraph} — execute a Cypher query and collect all records.
     * Each record is a Map whose keys are the Cypher RETURN column names.
     */
    public List<Map<String, Object>> query(String subgraph, String cypher, Map<String, Object> params)
            throws IOException {
        List<Map<String, Object>> records = new ArrayList<>();
        streamQuery(subgraph, cypher, params, records::add);
        return records;
    }

    /**
     * POST /{subgraph} — execute a Cypher query and stream records one at a time
     * via a consumer.  The HTTP response is read as NDJSON line-by-line so
     * memory usage stays bounded regardless of result size.
     */
    @SuppressWarnings("unchecked")
    public void streamQuery(String subgraph, String cypher, Map<String, Object> params,
                            Consumer<Map<String, Object>> consumer) throws IOException {
        String body = gson.toJson(Map.of("query", cypher, "params", params));

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/" + subgraph))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json")
                .build();

        HttpResponse<InputStream> resp;
        try {
            resp = httpClient.send(req, HttpResponse.BodyHandlers.ofInputStream());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during cypher query", e);
        }

        if (resp.statusCode() == 404) {
            throw new IOException("Subgraph not found: " + subgraph);
        }

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resp.body(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                Map<String, Object> record = gson.fromJson(line, Map.class);
                if (record.containsKey("_error")) {
                    throw new IOException("Cypher query error: " + record.get("_error"));
                }
                consumer.accept(record);
            }
        }
    }
}
