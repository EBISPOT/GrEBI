package uk.ac.ebi.grebi.repo;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.springframework.data.domain.Pageable;

import uk.ac.ebi.grebi.GrebiFacetedResultsPage;
import uk.ac.ebi.grebi.db.GrebiPostgresClient;
import uk.ac.ebi.grebi.db.ResolverClient;

/**
 * Repository for querying edges from PostgreSQL.
 * Replaces the edge-related methods that were previously in GrebiSolrRepo.
 */
public class GrebiPostgresRepo {

    private final GrebiPostgresClient pgClient;
    private final ResolverClient resolver = new ResolverClient();

    public GrebiPostgresRepo() {
        this.pgClient = new GrebiPostgresClient();
    }

    public Set<String> getSubgraphs() {
        return pgClient.getSubgraphs();
    }

    /**
     * Search edges with full resolution via the SQLite resolver.
     * Returns the complete edge JSON including _refs for from/to nodes.
     */
    public GrebiFacetedResultsPage<Map<String, Object>> searchEdgesPaginated(
            String subgraph, String filterField, String filterValue,
            Map<String, List<String>> extraFilters,
            String sortField, String sortDir,
            Pageable pageable) {

        var result = pgClient.queryEdges(subgraph, filterField, filterValue,
                extraFilters, sortField, sortDir,
                (int) pageable.getOffset(), pageable.getPageSize());

        return new GrebiFacetedResultsPage<>(
                result.results,
                new LinkedHashMap<>(),
                pageable,
                result.totalCount
        );
    }

    /**
     * Search edge refs (lightweight: type, datasources, fromNodeId, toNodeId + resolved node refs).
     */
    public GrebiFacetedResultsPage<Map<String, Object>> searchEdgeRefsPaginated(
            String subgraph, String filterField, String filterValue,
            Map<String, List<String>> extraFilters,
            String sortField, String sortDir,
            Pageable pageable) {

        var result = pgClient.queryEdgeRefs(subgraph, filterField, filterValue,
                extraFilters, sortField, sortDir,
                (int) pageable.getOffset(), pageable.getPageSize());

        // Collect unique from/to node IDs for resolution
        Set<String> nodeIds = new LinkedHashSet<>();
        for (var edge : result.results) {
            Object fromId = edge.get("grebi:fromNodeId");
            Object toId = edge.get("grebi:toNodeId");
            if (fromId instanceof String) nodeIds.add((String) fromId);
            if (toId instanceof String) nodeIds.add((String) toId);
        }

        // Resolve node IDs to lightweight refs
        Map<String, Map<String, Object>> nodeRefs = nodeIdsToNodeRefs(subgraph, nodeIds);

        // Attach from/to node refs
        List<Map<String, Object>> enriched = new ArrayList<>();
        for (var edge : result.results) {
            Map<String, Object> retEdge = new LinkedHashMap<>(edge);
            Object fromId = edge.get("grebi:fromNodeId");
            Object toId = edge.get("grebi:toNodeId");
            if (fromId instanceof String && nodeRefs.containsKey(fromId)) {
                retEdge.put("from", nodeRefs.get(fromId));
            }
            if (toId instanceof String && nodeRefs.containsKey(toId)) {
                retEdge.put("to", nodeRefs.get(toId));
            }
            enriched.add(retEdge);
        }

        return new GrebiFacetedResultsPage<>(
                enriched,
                new LinkedHashMap<>(),
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
     * Convert node IDs to lightweight node ref maps.
     */
    private Map<String, Map<String, Object>> nodeIdsToNodeRefs(String subgraph, Collection<String> nodeIds) {
        if (nodeIds.isEmpty()) return Collections.emptyMap();

        Map<String, Map<String, Object>> fullNodes = resolver.resolveToMap(subgraph, nodeIds);
        if (fullNodes == null) return Collections.emptyMap();

        Set<String> REF_FIELDS = Set.of(
                "grebi:nodeId", "grebi:name", "grebi:datasources",
                "grebi:type", "grebi:sourceIds", "ols:curie"
        );

        Map<String, Map<String, Object>> refs = new LinkedHashMap<>();
        for (var entry : fullNodes.entrySet()) {
            if (entry.getValue() == null) continue;
            Map<String, Object> ref = new LinkedHashMap<>();
            for (String field : REF_FIELDS) {
                if (entry.getValue().containsKey(field)) {
                    ref.put(field, entry.getValue().get(field));
                }
            }
            refs.put(entry.getKey(), ref);
        }
        return refs;
    }
}
