package uk.ac.ebi.grebi.repo;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import org.springframework.data.domain.Pageable;

import uk.ac.ebi.grebi.GrebiFacetedResultsPage;
import uk.ac.ebi.grebi.db.GrebiPostgresClient;

/**
 * Repository for querying edges, nodes, autocomplete, and materialised queries from PostgreSQL.
 */
public class GrebiPostgresRepo {

    private final GrebiPostgresClient pgClient;

    public GrebiPostgresRepo() {
        this.pgClient = new GrebiPostgresClient();
    }

    public GrebiPostgresClient getPgClient() {
        return pgClient;
    }

    public Set<String> getGraphs() {
        return pgClient.getGraphs();
    }

    /**
     * Search edges with all fields. The _refs JSONB column already contains
     * node metadata for referenced IDs; extract from/to from it.
     */
    public GrebiFacetedResultsPage<Map<String, Object>> searchEdgesPaginated(
            String graph, String filterField, String filterValue,
            Map<String, List<String>> extraFilters,
            String sortField, String sortDir,
            Pageable pageable) {

        var result = pgClient.queryEdges(graph, filterField, filterValue,
                extraFilters, sortField, sortDir,
                (int) pageable.getOffset(), pageable.getPageSize());

        List<Map<String, Object>> enriched = new ArrayList<>();
        for (var edge : result.results) {
            enriched.add(attachFromTo(edge));
        }

        return new GrebiFacetedResultsPage<>(
                enriched,
                result.facets,
                pageable,
                result.totalCount
        );
    }

    /**
     * Search edge refs (lightweight: type, datasources, fromNodeId, toNodeId + refs from _refs JSONB).
     */
    public GrebiFacetedResultsPage<Map<String, Object>> searchEdgeRefsPaginated(
            String graph, String filterField, String filterValue,
            Map<String, List<String>> extraFilters,
            String sortField, String sortDir,
            Pageable pageable) {

        var result = pgClient.queryEdgeRefs(graph, filterField, filterValue,
                extraFilters, sortField, sortDir,
                (int) pageable.getOffset(), pageable.getPageSize());

        List<Map<String, Object>> enriched = new ArrayList<>();
        for (var edge : result.results) {
            enriched.add(attachFromTo(edge));
        }

        return new GrebiFacetedResultsPage<>(
                enriched,
                result.facets,
                pageable,
                result.totalCount
        );
    }

    public Map<String, Map<String, Integer>> getIncomingEdgeCounts(String graph, String nodeId) {
        return pgClient.getEdgeCounts(graph, "grebi:toNodeId", nodeId);
    }

    public Map<String, Map<String, Integer>> getOutgoingEdgeCounts(String graph, String nodeId) {
        return pgClient.getEdgeCounts(graph, "grebi:fromNodeId", nodeId);
    }

    /**
     * Fetch both incoming and outgoing edge counts in parallel.
     */
    public Map<String, Map<String, Map<String, Integer>>> getBothEdgeCounts(String graph, String nodeId) {
        CompletableFuture<Map<String, Map<String, Integer>>> inFuture =
                CompletableFuture.supplyAsync(() -> getIncomingEdgeCounts(graph, nodeId));
        CompletableFuture<Map<String, Map<String, Integer>>> outFuture =
                CompletableFuture.supplyAsync(() -> getOutgoingEdgeCounts(graph, nodeId));

        Map<String, Map<String, Map<String, Integer>>> result = new LinkedHashMap<>();
        result.put("incoming", inFuture.join());
        result.put("outgoing", outFuture.join());
        return result;
    }

    /**
     * Get a single edge by its ID.
     */
    public Map<String, Object> getEdgeById(String graph, String edgeId) {
        return pgClient.getEdgeById(graph, edgeId);
    }

    /**
     * Search edges with optional filters (no required node ID).
     */
    public GrebiFacetedResultsPage<Map<String, Object>> searchEdges(
            String graph,
            Map<String, List<String>> filters,
            String sortField, String sortDir,
            Pageable pageable) {

        var result = pgClient.searchEdges(graph, filters, sortField, sortDir,
                (int) pageable.getOffset(), pageable.getPageSize());

        List<Map<String, Object>> enriched = new ArrayList<>();
        for (var edge : result.results) {
            enriched.add(attachFromTo(edge));
        }

        return new GrebiFacetedResultsPage<>(
                enriched,
                result.facets,
                pageable,
                result.totalCount
        );
    }

    /**
     * Extract from/to node refs from the _refs field stored in the edge row.
     * _refs is a TEXT column containing JSON, so it may arrive as a String
     * (from row_to_json) or a Map (from explicit parsing in queryEdgeRefs).
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> attachFromTo(Map<String, Object> edge) {
        Map<String, Object> retEdge = new LinkedHashMap<>(edge);
        Object refsObj = edge.get("_refs");
        if (refsObj instanceof String) {
            refsObj = new com.google.gson.Gson().fromJson((String) refsObj,
                    new com.google.gson.reflect.TypeToken<Map<String, Object>>() {}.getType());
            retEdge.put("_refs", refsObj);
        }
        if (refsObj instanceof Map) {
            Map<String, Object> refs = (Map<String, Object>) refsObj;
            Object fromId = edge.get("grebi:fromNodeId");
            Object toId = edge.get("grebi:toNodeId");
            if (fromId instanceof String) {
                retEdge.put("from", refs.get(fromId));
            }
            if (toId instanceof String) {
                retEdge.put("to", refs.get(toId));
            }
        }
        return retEdge;
    }

    /**
     * Search nodes by vector similarity using pgvector.
     */
    public List<GrebiPostgresClient.VectorSearchResult> searchByVector(
            String graph, String embeddingModel, float[] queryVector, int limit) {
        return pgClient.searchByVector(graph, embeddingModel, queryVector, limit);
    }

    /**
     * Get a node's embedding vector for a given model.
     */
    public float[] getNodeEmbedding(String graph, String nodeId, String embeddingModel) {
        return pgClient.getNodeEmbedding(graph, nodeId, embeddingModel);
    }

    /**
     * Autocomplete node labels using pg_trgm.
     */
    public List<String> autocomplete(String graph, String q) {
        return pgClient.autocomplete(graph, q);
    }

    /**
     * Search nodes by text (ILIKE on name) and optional filters.
     * When resolve=true, fetches full node blobs from the blobs table.
     */
    public GrebiFacetedResultsPage<Map<String, Object>> searchNodesPaginated(
            String graph, String q, Map<String, List<String>> filters,
            boolean resolve, Pageable pageable) {

        var result = pgClient.searchNodes(graph, q, filters,
                (int) pageable.getOffset(), pageable.getPageSize());

        List<Map<String, Object>> content;
        if (resolve && !result.results.isEmpty()) {
            var nodeIds = result.results.stream()
                    .map(r -> (String) r.get("grebi:nodeId"))
                    .toList();
            content = pgClient.resolveToList(graph, nodeIds);
        } else {
            content = result.results;
        }

        return new GrebiFacetedResultsPage<>(
                content, result.facets, pageable, result.totalCount);
    }

    /**
     * Search materialised query results with optional text search, filters, and facets.
     */
    public GrebiFacetedResultsPage<Map<String, Object>> searchMaterialisedQueryResultsPaginated(
            String graph, String queryId, String searchText,
            Map<String, List<String>> filters, List<String> facetFields,
            Pageable pageable) {

        var result = pgClient.searchMaterialisedQueryResults(graph, queryId, searchText,
                filters, facetFields, (int) pageable.getOffset(), pageable.getPageSize());

        return new GrebiFacetedResultsPage<>(
                result.results, result.facets, pageable, result.totalCount);
    }
}
