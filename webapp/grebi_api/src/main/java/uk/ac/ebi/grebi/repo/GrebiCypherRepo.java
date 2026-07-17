package uk.ac.ebi.grebi.repo;

import com.google.gson.Gson;

import jakarta.servlet.http.HttpServletResponse;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import uk.ac.ebi.grebi.GrebiApi;
import uk.ac.ebi.grebi.db.CypherServiceClient;
import uk.ac.ebi.grebi.db.PrefixService;
import uk.ac.ebi.grebi.db.GrebiPostgresClient;
import uk.ac.ebi.grebi.repo.QueryTemplate;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class GrebiCypherRepo {

    CypherServiceClient cypherClient;
    Set<String> graphs;

    private static final boolean DEBUG_QUERIES =
        "true".equalsIgnoreCase(System.getenv("GREBI_DEBUG_QUERIES"));

    GrebiPostgresClient pgClient = new GrebiPostgresClient();
    Gson gson = new Gson();
    PrefixService prefixClient = PrefixService.get();

    // Dedicated pool so the (potentially slow) count query can run concurrently
    // with the data query without contending for the shared ForkJoinPool.
    private final java.util.concurrent.ExecutorService queryExecutor =
        java.util.concurrent.Executors.newCachedThreadPool(r -> {
            var t = new Thread(r, "grebi-cypher-count");
            t.setDaemon(true);
            return t;
        });

    public GrebiCypherRepo() throws IOException {
        cypherClient = new CypherServiceClient(CypherServiceClient.getCypherServiceUrl());
        graphs = cypherClient.getGraphs();
    }

    public Set<String> getGraphs() {
        return graphs;
    }

    final String STATS_QUERY = new String(GrebiApi.class.getResourceAsStream("/cypher/stats.cypher").readAllBytes(), StandardCharsets.UTF_8);
    final String INCOMING_EDGES_QUERY = new String(GrebiApi.class.getResourceAsStream("/cypher/incoming_edges.cypher").readAllBytes(), StandardCharsets.UTF_8);

    @SuppressWarnings("unchecked")
    public Map<String, Map<String,Object>> getStats() {
        Map<String, Map<String,Object>> graphToStats = new HashMap<>();
        for(var graph : graphs) {
            try {
                var records = cypherClient.query(graph, STATS_QUERY, Map.of());
                if (!records.isEmpty()) {
                    graphToStats.put(graph, (Map<String, Object>) records.get(0).values().iterator().next());
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to get stats for graph " + graph, e);
            }
        }
        return graphToStats;
    }

    public class EdgeAndNode {
        public Map<String,Object> edge, node;
        public EdgeAndNode(Map<String,Object> edge, Map<String,Object> node) {
            this.edge = edge;
            this.node = node;
        }
    }

    public List<EdgeAndNode> getIncomingEdges(String graph, String nodeId, Pageable pageable) {
        List<Map<String, Object>> records;
        try {
            records = cypherClient.query(graph, INCOMING_EDGES_QUERY, Map.of(
                    "nodeId", graph + ":" + nodeId,
                    "offset", pageable.getOffset(),
                    "limit", pageable.getPageSize()
            ));
        } catch (IOException e) {
            throw new RuntimeException("Failed to get incoming edges", e);
        }

        var resolved = pgClient.resolveToMap(
                graph,
                records.stream().flatMap(record -> {
                    return List.of(
                            removeGraphPrefix((String) record.get("otherId"), graph),
                            removeGraphPrefix((String) record.get("edgeId"), graph)
                    ).stream();
                }).collect(Collectors.toSet()));

        return records.stream().map(record -> {
            var otherId = removeGraphPrefix((String) record.get("otherId"), graph);
            var edgeId = removeGraphPrefix((String) record.get("edgeId"), graph);
            return new EdgeAndNode(resolved.get(edgeId), resolved.get(otherId));
        }).collect(Collectors.toList());
    }

    public static class SimilarResult {
        public Object node;
        public double score;
    }

    public List<SimilarResult> getSimilar(String graph, String nodeId, int n) {

		String query = "MATCH (c:GraphNode {`grebi:nodeId`: $id}) "
		+ "CALL db.index.vector.queryNodes('embeddings', $n, c.`embedding:text-embedding-3-small`) "
		+ "YIELD node AS similar, score "
		+ "RETURN similar { .id, .`grebi:nodeId`, .`grebi:name`,`grebi:type`: labels(similar) } as node, score "
		+ "ORDER BY score DESC ";

		ArrayList<SimilarResult> res = new ArrayList<>();

        List<Map<String, Object>> records;
        try {
            records = cypherClient.query(graph, query, Map.of(
                "id", graph + ":" + nodeId,
                "n", n
            ));
        } catch (IOException e) {
            throw new RuntimeException("Failed to get similar nodes", e);
        }

		for(var rmap : records) {
            SimilarResult resRow = new SimilarResult();
			resRow.score = ((Number) rmap.get("score")).doubleValue();
			resRow.node = rmap.get("node");
			res.add(resRow);
		}

		return res;
    }

    public static class DirectionAndEdgeType {
        public String direction;
        public String edgeType;
    }

    /**
     * For each (direction, edgeType) pair where there is exactly one edge,
     * resolve the connected node. Uses a dynamically-constructed UNION ALL
     * Cypher query with literal relationship types for optimal planner performance.
     */
    public Map<String, Map<String, Object>> resolveSingleEdges(String graph, String nodeId, List<DirectionAndEdgeType> items) {
        if (items == null || items.isEmpty()) {
            return Collections.emptyMap();
        }

        String prefixedNodeId = graph + ":" + nodeId;

        // Build a UNION ALL query with one branch per item, using the Cypher DSL
        // for safe relationship type escaping. Each branch matches exactly one
        // relationship of the specified type and direction, returning the other node's ID.

        org.neo4j.cypherdsl.core.Node n = org.neo4j.cypherdsl.core.Cypher.node("GraphNode")
                .withProperties("grebi:nodeId", org.neo4j.cypherdsl.core.Cypher.parameter("nodeId"));
        org.neo4j.cypherdsl.core.Node other = org.neo4j.cypherdsl.core.Cypher.node("GraphNode").named("other");

        List<org.neo4j.cypherdsl.core.Statement> branches = new ArrayList<>();

        for (var item : items) {
            org.neo4j.cypherdsl.core.Statement branch;
            if ("incoming".equals(item.direction)) {
                branch = org.neo4j.cypherdsl.core.Cypher
                        .match(other.relationshipTo(n, item.edgeType))
                        .returning(
                                other.property("grebi:nodeId").as("otherId"),
                                other.property("grebi:name").as("otherName"),
                                org.neo4j.cypherdsl.core.Cypher.literalOf(item.direction).as("dir"),
                                org.neo4j.cypherdsl.core.Cypher.literalOf(item.edgeType).as("et")
                        )
                        .limit(1)
                        .build();
            } else {
                branch = org.neo4j.cypherdsl.core.Cypher
                        .match(n.relationshipTo(other, item.edgeType))
                        .returning(
                                other.property("grebi:nodeId").as("otherId"),
                                other.property("grebi:name").as("otherName"),
                                org.neo4j.cypherdsl.core.Cypher.literalOf(item.direction).as("dir"),
                                org.neo4j.cypherdsl.core.Cypher.literalOf(item.edgeType).as("et")
                        )
                        .limit(1)
                        .build();
            }
            branches.add(branch);
        }

        // Combine with UNION ALL (requires at least 2 statements)
        String cypher;
        if (branches.size() == 1) {
            cypher = branches.get(0).getCypher();
        } else {
            org.neo4j.cypherdsl.core.Statement combined = org.neo4j.cypherdsl.core.Cypher.unionAll(
                    branches.toArray(new org.neo4j.cypherdsl.core.Statement[0])
            );
            cypher = combined.getCypher();
        }

        List<Map<String, Object>> records;
        try {
            records = cypherClient.query(graph, cypher, Map.of("nodeId", prefixedNodeId));
        } catch (IOException e) {
            throw new RuntimeException("Failed to resolve single edges", e);
        }

        if (records.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Map<String, Object>> resultMap = new LinkedHashMap<>();
        for (var r : records) {
            String dir = (String) r.get("dir");
            String et = (String) r.get("et");
            String rawId = (String) r.get("otherId");
            String cleanId = removeGraphPrefix(rawId, graph);

            Map<String, Object> nodeData = new LinkedHashMap<>();
            nodeData.put("grebi:nodeId", cleanId);

            var otherName = r.get("otherName");
            if (otherName != null) {
                nodeData.put("grebi:name", otherName);
            }

            resultMap.put(dir + "::" + et, nodeData);
        }
        return resultMap;
    }

    private String removeGraphPrefix(String id, String graph) {
        if(!id.startsWith(graph + ":")) {
            throw new RuntimeException();
        }
        return id.substring(graph.length() + 1);
    }

    @SuppressWarnings("unchecked")
    private String extractCleanGraphNodeId(String graph, Object value) {
        if (value == null) {
            return null;
        }

        var node = (Map<String, Object>) value;
        var rawNodeId = node.get("grebi:nodeId");
        if (rawNodeId == null) {
            return null;
        }

        var nodeId = rawNodeId.toString();
        if (nodeId.startsWith(graph + ":")) {
            return nodeId.substring(graph.length() + 1);
        }
        return nodeId;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> normalizeGraphNodeValue(String graph, Object value) {
        if (value == null) {
            return null;
        }

        var node = new TreeMap<>((Map<String, Object>) value);
        var nodeId = extractCleanGraphNodeId(graph, value);
        if (nodeId != null) {
            node.put("grebi:nodeId", nodeId);
        }
        return node;
    }

    class PreparedQuery {
        public String query;
        public String countQuery;
        public Map<String, Object> params;
    }

    private QueryTemplate.ResultColumn getResultColumn(QueryTemplate template, String columnId) {
        return template.result_columns.stream()
            .filter(column -> column.column_id.equals(columnId))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Result column " + columnId + " not found"));
    }

    private String getSortExpression(QueryTemplate.ResultColumn column) {
        var columnType = column.column_type == null ? "" : column.column_type.toLowerCase();
        if (columnType.equals("float")) {
            return "toFloat(" + column.column_id + ")";
        }
        if (columnType.equals("int") || columnType.equals("integer")) {
            return "toInteger(" + column.column_id + ")";
        }
        return column.column_id;
    }

    static Object normalizeResultValue(QueryTemplate.ResultColumn column, Object value) {
        if (value == null || column == null || column.column_type == null) {
            return value;
        }

        var columnType = column.column_type.toLowerCase();

        switch (columnType) {
            case "float":
                return normalizeFloat(value, column.column_id);
            case "int":
            case "integer":
                return normalizeInteger(value, column.column_id);
            case "boolean":
                return normalizeBoolean(value, column.column_id);
            case "datasourcelist":
                return normalizeDatasourceList(value);
            case "string":
                return value instanceof String ? value : Objects.toString(value, null);
            default:
                return value;
        }
    }

    private static Double normalizeFloat(Object value, String columnId) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value instanceof String str) {
            if (str.isBlank()) {
                return null;
            }
            try {
                return Double.parseDouble(str);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Could not parse float value for column " + columnId + ": " + str, e);
            }
        }
        throw new IllegalArgumentException("Unsupported float value type for column " + columnId + ": " + value.getClass().getName());
    }

    private static Integer normalizeInteger(Object value, String columnId) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof String str) {
            if (str.isBlank()) {
                return null;
            }
            try {
                return Integer.parseInt(str);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Could not parse integer value for column " + columnId + ": " + str, e);
            }
        }
        throw new IllegalArgumentException("Unsupported integer value type for column " + columnId + ": " + value.getClass().getName());
    }

    private static Boolean normalizeBoolean(Object value, String columnId) {
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value instanceof String str) {
            if (str.equalsIgnoreCase("true")) {
                return true;
            }
            if (str.equalsIgnoreCase("false")) {
                return false;
            }
            if (str.isBlank()) {
                return null;
            }
        }
        throw new IllegalArgumentException("Unsupported boolean value for column " + columnId + ": " + value);
    }

    private static List<String> normalizeDatasourceList(Object value) {
        if (value instanceof List<?> list) {
            return list.stream()
                .filter(Objects::nonNull)
                .map(Object::toString)
                .toList();
        }
        if (value instanceof Collection<?> collection) {
            return collection.stream()
                .filter(Objects::nonNull)
                .map(Object::toString)
                .toList();
        }
        if (value instanceof String str) {
            if (str.isBlank()) {
                return List.of();
            }
            return List.of(str);
        }
        return List.of(value.toString());
    }

    PreparedQuery prepareQuery(
        String graph, 
        QueryTemplate template,
        Map<String, List<String>> params,
        Sort sort
    ) {

        if(!template.graphs.contains(graph)) {
            throw new IllegalArgumentException("Query template " + template.id + " is not available for graph " + graph);
        }

        for(var param : params.entrySet()) {
            if(!template.params.stream().anyMatch(p -> p.param_id.equals(param.getKey()))) {
                throw new IllegalArgumentException("Unknown parameter " + param.getKey() + " provided for query template " + template.id + "; valid parameters are: " +
                    template.params.stream().map(p -> p.param_id).collect(Collectors.joining(", ")));
            }
        }

        Map<String, Object> paramMap = new HashMap<>();
        for (QueryTemplate.Parameter p : template.params) {

            var values = params.get(p.param_id);

            if(values == null || values.isEmpty()) {
                if(p.param_default != null) {
                    values = List.of(p.param_default);
                } else {
                    throw new IllegalArgumentException("Parameter " + p.param_id + " is required but not provided");
                }
            }

            if(p.param_type.equals("SourceId")) {
                if(values.size() > 1) {
                    throw new IllegalArgumentException("SourceId param " + p.param_id + " cannot have multiple values");
                }
                var nodeId = values.get(0);
                nodeId = prefixClient.reprefix(List.of(nodeId)).get(0);
                paramMap.put(p.param_id, nodeId);
            } else if(p.param_type.equals("string")) {

                if(values.size() > 1) {
                    throw new IllegalArgumentException("String param " + p.param_id + " cannot have multiple values");
                }
                var stringValue = values.get(0);
                paramMap.put(p.param_id, stringValue);

            } else if(p.param_type.equals("float")) {

                if(values.size() > 1) {
                    throw new IllegalArgumentException("Float param " + p.param_id + " cannot have multiple values");
                }
                try {
                    var floatValue = Double.parseDouble(values.get(0));
                    paramMap.put(p.param_id, floatValue);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid float value for parameter " + p.param_id + ": " + values.get(0));
                }

            } else {
                throw new IllegalArgumentException("Unknown parameter type " + p.param_type + " for parameter " + p.param_id);
            }
        }

        PreparedQuery preparedQuery = new PreparedQuery();

        preparedQuery.query = template.cypher_match_fragment.trim()
                + "\n" + template.cypher_return_fragment.trim();
        preparedQuery.countQuery = template.cypher_match_fragment.trim() + "\n" + template.cypher_count_fragment.trim();
        preparedQuery.params = paramMap;

        if(sort != null && !sort.isUnsorted()) {
            var sorts = sort.stream().collect(Collectors.toList());
            if(sorts.size() != 1) {
                throw new IllegalArgumentException("Sorting by multiple columns is not supported");
            }
            var sortField = sorts.get(0).getProperty();
            if(!template.result_columns.stream().anyMatch(c -> c.column_id.equals(sortField))) {
                throw new IllegalArgumentException("Sort column " + sortField + " not found; valid columns are: " +
                    template.result_columns.stream().map(c -> c.column_id).collect(Collectors.joining(", ")));
            }
            var sortColumn = getResultColumn(template, sortField);
            var sortExpression = getSortExpression(sortColumn);
            if(sorts.get(0).isAscending()) {
                preparedQuery.query += "\nORDER BY " + sortExpression + " ASC";
            } else {
                preparedQuery.query += "\nORDER BY " + sortExpression + " DESC";
            }
        }

        return preparedQuery;
    }


    public Page<Map<String,Object>> runQueryFromTemplatePaginated(
        String graph,
        QueryTemplate template,
        Map<String, List<String>> params,
        boolean resolve,
        Pageable pageable
        ) {
        return runQueryFromTemplatePaginated(graph, template, params, resolve, pageable, null);
    }

    /**
     * @param overrideCount if non-null, the total is taken from this value and the
     *   (expensive) live count query is skipped. Used to serve a counts_only
     *   materialised template's data live while its total comes from Postgres.
     */
    public Page<Map<String,Object>> runQueryFromTemplatePaginated(
        String graph,
        QueryTemplate template,
        Map<String, List<String>> params,
        boolean resolve,
        Pageable pageable,
        Long overrideCount
        ) {

        var preparedQuery = prepareQuery(graph, template, params, pageable.getSort());
        var query = preparedQuery.query;
        var countQuery = preparedQuery.countQuery;
        var paramMap = preparedQuery.params;

        query = query + "\nSKIP " + pageable.getOffset()
                + "\nLIMIT " + pageable.getPageSize();

        if (DEBUG_QUERIES) {
            System.err.println("Running query: " + query + "\nWith parameters: " + paramMap + "\nCount query: " + countQuery);
        }

        // Run the data and (unbounded) count queries concurrently rather than
        // sequentially — for high-fan-out templates the count is as expensive as
        // the data query, so serialising them roughly doubled wall-clock latency.
        // When overrideCount is supplied (counts_only serving) skip the count query.
        final String fCountQuery = countQuery;
        final Map<String, Object> fParamMap = paramMap;
        CompletableFuture<List<Map<String, Object>>> countFuture =
            overrideCount != null ? null :
            CompletableFuture.supplyAsync(() -> {
                try {
                    return cypherClient.query(graph, fCountQuery, fParamMap);
                } catch (IOException e) {
                    throw new java.util.concurrent.CompletionException(e);
                }
            }, queryExecutor);

        List<Map<String, Object>> records;
        try {
            records = cypherClient.query(graph, query, paramMap);
        } catch (IOException e) {
            if (countFuture != null) countFuture.cancel(true);
            throw new RuntimeException("Failed to run query template", e);
        }

        int count;
        if (overrideCount != null) {
            count = overrideCount.intValue();
        } else {
            List<Map<String, Object>> countRecords;
            try {
                countRecords = countFuture.join();
            } catch (java.util.concurrent.CompletionException e) {
                throw new RuntimeException("Failed to run count query", e.getCause());
            }
            if(countRecords.isEmpty() || countRecords.get(0).get("count") == null) {
                throw new RuntimeException("Count query did not return a count");
            }
            count = ((Number) countRecords.get(0).get("count")).intValue();
        }
        if(count == 0) {
            return new org.springframework.data.domain.PageImpl<>(List.of(), pageable, 0);
        }

        List<QueryTemplate.ResultColumn> columns = template.result_columns;

        if(resolve) {

            var resolved = pgClient.resolveToMap(
                graph,
                records.stream()
                    .flatMap(record -> columns.stream()
                        .filter(column -> column.column_type.equals("GraphNodeId"))
                        .map(column -> extractCleanGraphNodeId(graph, record.get(column.column_id)))
                        .filter(Objects::nonNull)
                    )
                    .collect(Collectors.toSet())
            );

            var results =  records.stream().map(record -> {
                Map<String, Object> row = new LinkedHashMap<>();
                for (QueryTemplate.ResultColumn column : columns) {
                    String columnId = column.column_id;
                    if (column.column_type.equals("GraphNodeId")) {
                        var nodeId = extractCleanGraphNodeId(graph, record.get(columnId));
                        row.put(columnId, nodeId == null ? null : resolved.get(nodeId));
                    } else {
                        row.put(columnId, normalizeResultValue(column, record.get(columnId)));
                    }
                }
                return row;
            }).collect(Collectors.toList());

            return new PageImpl<Map<String, Object>>(
                results,
                pageable,
                count
            );

        } else {
            return new PageImpl<Map<String, Object>>(
                records.stream()
                    .map(record -> {
                        Map<String, Object> row = new LinkedHashMap<>();

                        for (QueryTemplate.ResultColumn column : columns) {
                            String columnId = column.column_id;
                            if (column.column_type.equals("GraphNodeId")) {
                                row.put(columnId, normalizeGraphNodeValue(graph, record.get(columnId)));
                            } else {
                                row.put(columnId, normalizeResultValue(column, record.get(columnId)));
                            }
                        }

                        return row;
                    })
                    .collect(Collectors.toList()),
                pageable,
                count
            );
        }
    }


    /** CSV header cells for a template's result columns (EdgeId/EdgeProps omitted;
     *  a GraphNodeId column expands to {id}_id and {id}_label). Shared by the live
     *  and Postgres-materialised CSV paths. */
    public static List<String> csvHeader(List<QueryTemplate.ResultColumn> columns) {
        var csvColumns = new ArrayList<String>();
        for (QueryTemplate.ResultColumn column : columns) {
            if ("EdgeId".equals(column.column_type) || "EdgeProps".equals(column.column_type)) {
                continue;
            }
            if ("GraphNodeId".equals(column.column_type)) {
                csvColumns.add(column.column_id + "_id");
                csvColumns.add(column.column_id + "_label");
            } else {
                csvColumns.add(column.column_id);
            }
        }
        return csvColumns;
    }

    /** Write one CSV row for the given result columns (must match {@link #csvHeader}).
     *  `record` is a row map of column_id -> value (node columns are node-object
     *  maps with `id` and `grebi:name`). */
    @SuppressWarnings("unchecked")
    public static void writeCsvRow(List<QueryTemplate.ResultColumn> columns,
                                   Map<String, Object> record, PrintWriter writer) {
        boolean first = true;
        for (QueryTemplate.ResultColumn column : columns) {
            if ("EdgeId".equals(column.column_type) || "EdgeProps".equals(column.column_type)) {
                continue;
            }
            if (first) {
                first = false;
            } else {
                writer.write(",");
            }
            if ("GraphNodeId".equals(column.column_type)) {
                var value = (Map<String, Object>) record.get(column.column_id);
                if (value == null) {
                    writer.write("\"\",\"\"");
                    continue;
                }
                var sourceIds = (List<String>) value.get("id");
                var nodeId = pickFavouriteSourceId(sourceIds);
                var names = (List<String>) value.get("grebi:name");
                String nodeLabel = (names == null || names.isEmpty())
                        ? nodeId : Objects.toString(names.get(0), null);
                writer.write("\"" + (nodeId == null ? "" : nodeId.replace("\"", "\"\"")) + "\",");
                writer.write("\"" + (nodeLabel == null ? "" : nodeLabel.replace("\"", "\"\"")) + "\"");
            } else {
                String raw = Objects.toString(normalizeResultValue(column, record.get(column.column_id)), "");
                writer.write("\"" + raw.replace("\"", "\"\"") + "\"");
            }
        }
        writer.write("\n");
    }

    public CompletableFuture<Void> runQueryFromTemplateStreamed(
            String graph,
            QueryTemplate template,
            Map<String, List<String>> params,
            Sort sort,
            PrintWriter writer
    ) throws IOException {

        List<QueryTemplate.ResultColumn> columns = template.result_columns;

        writer.write(String.join(",", csvHeader(columns)));
        writer.write("\n");

        var preparedQuery = prepareQuery(graph, template, params, sort);

        return CompletableFuture.runAsync(() -> {
            try {
                cypherClient.streamQuery(graph, preparedQuery.query, preparedQuery.params,
                        record -> writeCsvRow(columns, record, writer));
                writer.flush();
            } catch (Exception e) {
                writer.write("ERROR: " + e.getMessage() + "\n");
                writer.flush();
                throw new RuntimeException(e);
            }
        });
    }

    public CompletableFuture<Void> runQueryFromTemplateStreamed(
            String graph,
            QueryTemplate template,
            Map<String, List<String>> params,
            Sort sort,
            HttpServletResponse res
    ) throws IOException {

        res.setContentType("text/csv");
        res.setCharacterEncoding("UTF-8");
        res.setHeader("Content-Disposition", "attachment; filename=\"" + template.id + ".csv\"");
        res.setStatus(HttpServletResponse.SC_OK);

        PrintWriter writer = res.getWriter();

        return runQueryFromTemplateStreamed(graph, template, params, sort, writer);
    }



    // TODO: move to config
    //
    private static final List<String> FAVOURITE_PREFIXES = List.of(
        "grebi:",
        "biolink:",
        "ro:",
        "hp:",
        "mp:",
        "mondo:",
        "oba:",
        "efo:",
        "doid:",
        "hgnc:",
        "mgi:",
        "uniprot:",
        "pmid:",
        "chebi:",
        "MTBLS",
        "MTBLC"
    );

    private static String pickFavouriteSourceId(List<String> ids) {

        if(ids == null || ids.isEmpty()) {
            return null;
        }

        for(String prefix : FAVOURITE_PREFIXES) {
            for(String id : ids) {
                    if(id.startsWith(prefix)) {
                        return id;
                    }
                }
            }
        

        return ids.get(0);
    }




}
