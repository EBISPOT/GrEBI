package uk.ac.ebi.grebi.repo;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import org.springframework.data.domain.Pageable;

import uk.ac.ebi.grebi.GrebiFacetedResultsPage;
import uk.ac.ebi.grebi.db.GrebiPostgresClient;

/**
 * Repository for querying edges from PostgreSQL.
 * Replaces the edge-related methods that were previously in GrebiSolrRepo.
 */
public class GrebiPostgresRepo {

    private final GrebiPostgresClient pgClient;

    public GrebiPostgresRepo() {
        this.pgClient = new GrebiPostgresClient();
    }

    public Set<String> getSubgraphs() {
        return pgClient.getSubgraphs();
    }

    /**
     * Search edges with all fields. The _refs JSONB column already contains
     * node metadata for referenced IDs; extract from/to from it.
     */
    public GrebiFacetedResultsPage<Map<String, Object>> searchEdgesPaginated(
            String subgraph, String filterField, String filterValue,
            Map<String, List<String>> extraFilters,
            String sortField, String sortDir,
            Pageable pageable) {

        var result = pgClient.queryEdges(subgraph, filterField, filterValue,
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
            String subgraph, String filterField, String filterValue,
            Map<String, List<String>> extraFilters,
            String sortField, String sortDir,
            Pageable pageable) {

        var result = pgClient.queryEdgeRefs(subgraph, filterField, filterValue,
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

    public Map<String, Map<String, Integer>> getIncomingEdgeCounts(String subgraph, String nodeId) {
        return pgClient.getEdgeCounts(subgraph, "grebi:toNodeId", nodeId);
    }

    public Map<String, Map<String, Integer>> getOutgoingEdgeCounts(String subgraph, String nodeId) {
        return pgClient.getEdgeCounts(subgraph, "grebi:fromNodeId", nodeId);
    }

    /**
     * Fetch both incoming and outgoing edge counts in parallel.
     */
    public Map<String, Map<String, Map<String, Integer>>> getBothEdgeCounts(String subgraph, String nodeId) {
        CompletableFuture<Map<String, Map<String, Integer>>> inFuture =
                CompletableFuture.supplyAsync(() -> getIncomingEdgeCounts(subgraph, nodeId));
        CompletableFuture<Map<String, Map<String, Integer>>> outFuture =
                CompletableFuture.supplyAsync(() -> getOutgoingEdgeCounts(subgraph, nodeId));

        Map<String, Map<String, Map<String, Integer>>> result = new LinkedHashMap<>();
        result.put("incoming", inFuture.join());
        result.put("outgoing", outFuture.join());
        return result;
    }

    /**
     * Get a single edge by its ID.
     */
    public Map<String, Object> getEdgeById(String subgraph, String edgeId) {
        return pgClient.getEdgeById(subgraph, edgeId);
    }

    /**
     * Search edges with optional filters (no required node ID).
     */
    public GrebiFacetedResultsPage<Map<String, Object>> searchEdges(
            String subgraph,
            Map<String, List<String>> filters,
            String sortField, String sortDir,
            Pageable pageable) {

        var result = pgClient.searchEdges(subgraph, filters, sortField, sortDir,
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
            String subgraph, String embeddingModel, float[] queryVector, int limit) {
        return pgClient.searchByVector(subgraph, embeddingModel, queryVector, limit);
    }

    /**
     * Get a node's embedding vector for a given model.
     */
    public float[] getNodeEmbedding(String subgraph, String nodeId, String embeddingModel) {
        return pgClient.getNodeEmbedding(subgraph, nodeId, embeddingModel);
    }
}
