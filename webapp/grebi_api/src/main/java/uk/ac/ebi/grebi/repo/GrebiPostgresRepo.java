package uk.ac.ebi.grebi.repo;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.io.PrintWriter;

import uk.ac.ebi.grebi.GrebiFacetedResultsPage;
import uk.ac.ebi.grebi.db.GrebiPostgresClient;

/**
 * Repository for querying edges, nodes, autocomplete, and materialised queries from PostgreSQL.
 */
public class GrebiPostgresRepo {

    private static final Logger logger = LoggerFactory.getLogger(GrebiPostgresRepo.class);
    private static final String INCOMING = "incoming";
    private static final String OUTGOING = "outgoing";

    private final GrebiPostgresClient pgClient;
    private final ExecutorService exampleEdgeCountWarmupExecutor;
    private final AtomicLong exampleEdgeCountWarmupGeneration = new AtomicLong();
    private volatile Map<ExampleNodeCacheKey, Map<String, Map<String, Map<String, Integer>>>> exampleEdgeCountCache = Map.of();

    private record ExampleNodeCacheKey(String graph, String nodeId) {}

    public GrebiPostgresRepo() {
        this.pgClient = new GrebiPostgresClient();
        this.exampleEdgeCountWarmupExecutor = Executors.newSingleThreadExecutor(r -> {
            var thread = new Thread(r, "example-edge-count-prewarm");
            thread.setDaemon(true);
            return thread;
        });
    }

    public GrebiPostgresClient getPgClient() {
        return pgClient;
    }

    public Set<String> getGraphs() {
        return pgClient.getGraphs();
    }

    public void refreshExampleEdgeCountCacheAsync(List<QueryTemplate> templates) {
        var templateSnapshot = templates == null ? List.<QueryTemplate>of() : List.copyOf(templates);
        long generation = exampleEdgeCountWarmupGeneration.incrementAndGet();
        logger.info("Queueing example edge-count cache rebuild #{} from {} templates",
                generation, templateSnapshot.size());
        exampleEdgeCountWarmupExecutor.submit(() -> rebuildExampleEdgeCountCache(templateSnapshot, generation));
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
        var cached = getCachedBothEdgeCounts(graph, nodeId);
        if (cached != null) {
            return cached.get(INCOMING);
        }
        return pgClient.getEdgeCounts(graph, "grebi:toNodeId", nodeId);
    }

    public Map<String, Map<String, Integer>> getOutgoingEdgeCounts(String graph, String nodeId) {
        var cached = getCachedBothEdgeCounts(graph, nodeId);
        if (cached != null) {
            return cached.get(OUTGOING);
        }
        return pgClient.getEdgeCounts(graph, "grebi:fromNodeId", nodeId);
    }

    /**
     * Fetch both incoming and outgoing edge counts in parallel.
     */
    public Map<String, Map<String, Map<String, Integer>>> getBothEdgeCounts(String graph, String nodeId) {
        var cached = getCachedBothEdgeCounts(graph, nodeId);
        if (cached != null) {
            return cached;
        }

        CompletableFuture<Map<String, Map<String, Integer>>> inFuture =
                CompletableFuture.supplyAsync(() -> getIncomingEdgeCounts(graph, nodeId));
        CompletableFuture<Map<String, Map<String, Integer>>> outFuture =
                CompletableFuture.supplyAsync(() -> getOutgoingEdgeCounts(graph, nodeId));

        Map<String, Map<String, Map<String, Integer>>> result = new LinkedHashMap<>();
        result.put(INCOMING, inFuture.join());
        result.put(OUTGOING, outFuture.join());
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

    /**
     * Serve a full-materialise parameterised template from Postgres with
     * closure-at-query-time. Returns the same page shape as the live Cypher path
     * (projected to result_columns; node columns resolved when resolve=true).
     */
    public GrebiFacetedResultsPage<Map<String, Object>> runMaterialisedParameterisedPaginated(
            String graph, QueryTemplate template, Map<String, List<String>> params,
            boolean resolve, Pageable pageable) {

        if (template.graphs != null && !template.graphs.contains(graph)) {
            throw new IllegalArgumentException(
                    "Query template " + template.id + " is not available for graph " + graph);
        }
        for (var key : params.keySet()) {
            if (template.params.stream().noneMatch(p -> p.param_id.equals(key))) {
                throw new IllegalArgumentException(
                        "Unknown parameter " + key + " provided for query template " + template.id);
            }
        }

        var closureParams = buildClosureParams(template, params);

        String sortColumn = null;
        boolean sortAsc = true;
        boolean sortNumeric = false;
        if (pageable.getSort() != null && pageable.getSort().isSorted()) {
            var order = pageable.getSort().iterator().next();
            sortColumn = order.getProperty();
            sortAsc = order.isAscending();
            final String sc = sortColumn;
            var col = template.result_columns.stream()
                    .filter(c -> c.column_id.equals(sc)).findFirst().orElse(null);
            if (col == null) {
                throw new IllegalArgumentException("Sort column " + sortColumn + " not found");
            }
            String ct = col.column_type == null ? "" : col.column_type.toLowerCase();
            sortNumeric = ct.equals("float") || ct.equals("int") || ct.equals("integer");
        }

        var result = pgClient.searchMaterialisedParameterised(
                graph, template.id, closureParams,
                sortColumn, sortAsc, sortNumeric,
                (int) pageable.getOffset(), pageable.getPageSize());

        var nodeColumns = template.result_columns.stream()
                .filter(c -> "GraphNodeId".equals(c.column_type))
                .map(c -> c.column_id).toList();

        Map<String, Map<String, Object>> resolved = Map.of();
        if (resolve && !result.results.isEmpty()) {
            Set<String> nodeIds = new HashSet<>();
            for (var raw : result.results) {
                for (var col : nodeColumns) {
                    if (raw.get(col) instanceof Map<?, ?> m && m.get("grebi:nodeId") instanceof String s) {
                        nodeIds.add(s);
                    }
                }
            }
            resolved = pgClient.resolveToMap(graph, nodeIds);
        }

        // Project each stored row down to the declared result columns so the shape
        // matches the live path exactly (dropping internal _refs/_node_ids/id).
        // Non-node columns are normalized to their declared type exactly as the
        // live Cypher path does (e.g. a float column stored as a JSON string
        // becomes a number), so the two backends return identical JSON.
        List<Map<String, Object>> content = new ArrayList<>();
        for (var raw : result.results) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (var col : template.result_columns) {
                Object v = raw.get(col.column_id);
                if ("GraphNodeId".equals(col.column_type)) {
                    if (resolve && v instanceof Map<?, ?> m
                            && m.get("grebi:nodeId") instanceof String s
                            && resolved.containsKey(s)) {
                        row.put(col.column_id, resolved.get(s));
                    } else {
                        row.put(col.column_id, v);
                    }
                } else {
                    row.put(col.column_id, GrebiCypherRepo.normalizeResultValue(col, v));
                }
            }
            content.add(row);
        }

        return new GrebiFacetedResultsPage<>(content, result.facets, pageable, result.totalCount);
    }

    /**
     * Exact serving-count for a counts_only materialised template: the summed
     * per-base `_count` histogram over the closure (data is served live).
     */
    public long materialisedParameterisedCount(
            String graph, QueryTemplate template, Map<String, List<String>> params) {
        // Only the counts_only serving path needs a standalone count — full mode
        // takes its total from the data page. The total is the summed per-base
        // histogram over the closure.
        return pgClient.sumMaterialisedParameterisedCounts(
                graph, template.id, buildClosureParams(template, params));
    }

    /**
     * Stream a full-materialise parameterised template as CSV from Postgres,
     * paging through the closure-filtered rows. Same CSV shape as the live path.
     */
    public void streamMaterialisedParameterisedCsv(
            String graph, QueryTemplate template, Map<String, List<String>> params,
            Sort sort, PrintWriter writer) {

        var closureParams = buildClosureParams(template, params);

        String sortColumn = null;
        boolean sortAsc = true;
        boolean sortNumeric = false;
        if (sort != null && sort.isSorted()) {
            var order = sort.iterator().next();
            final String sc = order.getProperty();
            var col = template.result_columns.stream()
                    .filter(c -> c.column_id.equals(sc)).findFirst().orElse(null);
            if (col != null) {
                sortColumn = sc;
                sortAsc = order.isAscending();
                String ct = col.column_type == null ? "" : col.column_type.toLowerCase();
                sortNumeric = ct.equals("float") || ct.equals("int") || ct.equals("integer");
            }
        }

        writer.write(String.join(",", GrebiCypherRepo.csvHeader(template.result_columns)));
        writer.write("\n");

        pgClient.streamMaterialisedParameterised(
                graph, template.id, closureParams, sortColumn, sortAsc, sortNumeric,
                row -> GrebiCypherRepo.writeCsvRow(template.result_columns, row, writer));
        writer.flush();
    }

    private List<GrebiPostgresClient.ClosureParam> buildClosureParams(
            QueryTemplate template, Map<String, List<String>> params) {
        if (template.materialise == null || template.materialise.params == null) {
            throw new IllegalStateException(
                    "Template " + template.id + " has no materialise params; cannot serve from Postgres");
        }
        var prefixService = uk.ac.ebi.grebi.db.PrefixService.get();
        List<GrebiPostgresClient.ClosureParam> out = new ArrayList<>();
        for (var mp : template.materialise.params) {
            var values = params.get(mp.param_id);
            var tp = template.params.stream()
                    .filter(p -> p.param_id.equals(mp.param_id)).findFirst().orElse(null);
            if (values == null || values.isEmpty()) {
                if (tp != null && tp.param_default != null) {
                    values = List.of(tp.param_default);
                } else {
                    throw new IllegalArgumentException(
                            "Parameter " + mp.param_id + " is required but not provided");
                }
            }
            String curie = prefixService.reprefix(List.of(values.get(0))).get(0);
            out.add(new GrebiPostgresClient.ClosureParam(mp.filters_column, mp.getClosure(), curie));
        }
        return out;
    }

    private Map<String, Map<String, Map<String, Integer>>> getCachedBothEdgeCounts(String graph, String nodeId) {
        return exampleEdgeCountCache.get(new ExampleNodeCacheKey(graph, nodeId));
    }

    private void rebuildExampleEdgeCountCache(List<QueryTemplate> templates, long generation) {
        long startNanos = System.nanoTime();
        try {
            var sourceIdsByGraph = collectExampleSourceIdsByGraph(templates, getGraphs());
            int exampleSourceIdCount = sourceIdsByGraph.values().stream().mapToInt(Set::size).sum();
            logger.info("Starting example edge-count cache rebuild #{} for {} example source IDs across {} graphs ({})",
                    generation, exampleSourceIdCount, sourceIdsByGraph.size(), summariseGraphSourceIds(sourceIdsByGraph));

            Map<ExampleNodeCacheKey, Map<String, Map<String, Map<String, Integer>>>> nextCache = new LinkedHashMap<>();
            for (var graphEntry : sourceIdsByGraph.entrySet()) {
                String graph = graphEntry.getKey();
                long graphStartNanos = System.nanoTime();
                int resolvedNodeCount = 0;
                int skippedSourceIdCount = 0;

                for (String sourceId : graphEntry.getValue()) {
                    String nodeId = resolveExampleSourceIdToNodeId(graph, sourceId);
                    if (nodeId == null) {
                        skippedSourceIdCount += 1;
                        logger.warn("Skipping edge-count prewarm for {}:{} because no node was found", graph, sourceId);
                        continue;
                    }

                    var cacheKey = new ExampleNodeCacheKey(graph, nodeId);
                    if (!nextCache.containsKey(cacheKey)) {
                        nextCache.put(cacheKey, loadBothEdgeCounts(graph, nodeId));
                        resolvedNodeCount += 1;
                    }
                }

                logger.info("Finished prewarming graph {} for rebuild #{}: {} example source IDs, {} unique nodes, {} skipped, {} ms",
                        graph,
                        generation,
                        graphEntry.getValue().size(),
                        resolvedNodeCount,
                        skippedSourceIdCount,
                        elapsedMillis(graphStartNanos));
            }

            int previousSize = exampleEdgeCountCache.size();
            exampleEdgeCountCache = Map.copyOf(nextCache);
            logger.info("Completed example edge-count cache rebuild #{} in {} ms: {} cached nodes (was {})",
                    generation,
                    elapsedMillis(startNanos),
                    exampleEdgeCountCache.size(),
                    previousSize);
        } catch (Exception e) {
            logger.error("Failed to rebuild example edge-count cache #{} after {} ms",
                    generation, elapsedMillis(startNanos), e);
        }
    }

    private static Map<String, Set<String>> collectExampleSourceIdsByGraph(
            List<QueryTemplate> templates,
            Set<String> availableGraphs) {
        Map<String, Set<String>> sourceIdsByGraph = new LinkedHashMap<>();
        if (templates == null || templates.isEmpty()) {
            return sourceIdsByGraph;
        }

        for (var template : templates) {
            if (template == null || template.params == null || template.params.isEmpty()
                    || template.examples == null || template.examples.isEmpty()) {
                continue;
            }

            String firstParamId = template.params.get(0).param_id;
            if (firstParamId == null || firstParamId.isBlank()) {
                continue;
            }

            Collection<String> graphs = (template.graphs == null || template.graphs.isEmpty())
                    ? availableGraphs
                    : template.graphs;

            for (String graph : graphs) {
                if (graph == null || graph.isBlank() || !availableGraphs.contains(graph)) {
                    continue;
                }

                var graphSourceIds = sourceIdsByGraph.computeIfAbsent(graph, ignored -> new LinkedHashSet<>());
                for (var example : template.examples) {
                    if (example == null || example.params == null) {
                        continue;
                    }
                    String sourceId = example.params.get(firstParamId);
                    if (sourceId != null && !sourceId.isBlank()) {
                        graphSourceIds.add(sourceId);
                    }
                }
            }
        }

        return sourceIdsByGraph;
    }

    private String resolveExampleSourceIdToNodeId(String graph, String sourceId) {
        Map<String, List<String>> filters = new LinkedHashMap<>();
        filters.put("grebi:sourceIds", List.of(sourceId));
        var result = pgClient.searchNodes(graph, null, filters, 0, 1);
        if (result.results.isEmpty()) {
            return null;
        }
        return (String) result.results.get(0).get("grebi:nodeId");
    }

    private Map<String, Map<String, Map<String, Integer>>> loadBothEdgeCounts(String graph, String nodeId) {
        Map<String, Map<String, Map<String, Integer>>> result = new LinkedHashMap<>();
        result.put(INCOMING, pgClient.getEdgeCounts(graph, "grebi:toNodeId", nodeId));
        result.put(OUTGOING, pgClient.getEdgeCounts(graph, "grebi:fromNodeId", nodeId));
        return result;
    }

    private static String summariseGraphSourceIds(Map<String, Set<String>> sourceIdsByGraph) {
        if (sourceIdsByGraph.isEmpty()) {
            return "no example source IDs";
        }

        List<String> parts = new ArrayList<>();
        for (var entry : sourceIdsByGraph.entrySet()) {
            parts.add(entry.getKey() + "=" + entry.getValue().size());
        }
        return String.join(", ", parts);
    }

    private static long elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }
}
