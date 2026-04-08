package uk.ac.ebi.grebi.db;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.reflect.TypeToken;
import org.jooq.*;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.io.ByteArrayInputStream;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.InflaterInputStream;

import static org.jooq.impl.DSL.*;

/**
 * Low-level PostgreSQL client for GrEBI edge queries.
 * Uses jOOQ DSL for safe SQL generation — no string-concatenated SQL.
 */
public class GrebiPostgresClient {

    private static final Logger logger = LoggerFactory.getLogger(GrebiPostgresClient.class);
    private final Gson gson = new Gson();

    private static final Set<String> ALLOWED_COLUMNS = Set.of(
            "grebi:edgeId", "grebi:type", "grebi:fromNodeId", "grebi:toNodeId", "grebi:subgraph"
    );

    private static final Set<String> ALLOWED_ARRAY_COLUMNS = Set.of(
            "grebi:datasources"
    );

    private static final Set<String> ALLOWED_ARRAY_CONTAINS_COLUMNS = Set.of(
            "grebi:datasources"
    );

    private static final List<String> EDGE_FACET_FIELDS = List.of("grebi:datasources");

    private static final Set<String> ALLOWED_NODE_COLUMNS = Set.of(
            "grebi:nodeId", "grebi:name", "ols:curie"
    );

    private static final Set<String> ALLOWED_NODE_ARRAY_COLUMNS = Set.of(
            "grebi:type", "grebi:datasources", "grebi:sourceIds"
    );

    private static final List<String> NODE_FACET_FIELDS = List.of("grebi:type", "grebi:datasources");

    private static final Field<String> GREBI_TYPE = field(name("grebi:type"), String.class);
    private static final Field<String> GREBI_FROM_NODE_ID = field(name("grebi:fromNodeId"), String.class);
    private static final Field<String> GREBI_TO_NODE_ID = field(name("grebi:toNodeId"), String.class);
    private static final Field<String> GREBI_REFS = field(name("_refs"), String.class);

    private final String host;
    private final String port;
    private final String user;
    private final String dbName;
    private Connection connection;

    public GrebiPostgresClient() {
        this.host = getEnvOrDefault("GREBI_POSTGRES_HOST", "localhost");
        this.port = getEnvOrDefault("GREBI_POSTGRES_PORT", "5432");
        this.user = getEnvOrDefault("GREBI_POSTGRES_USER", "grebi");
        this.dbName = getEnvOrDefault("GREBI_POSTGRES_DB", "grebi");

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            logger.error("PostgreSQL JDBC driver not found on classpath", e);
        }
    }

    private static String getEnvOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return val != null ? val : defaultValue;
    }

    private String getJdbcUrl() {
        return "jdbc:postgresql://" + host + ":" + port + "/" + dbName;
    }

    public synchronized Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            logger.info("Connecting to PostgreSQL at {}", getJdbcUrl());
            connection = DriverManager.getConnection(getJdbcUrl(), user, "");
        }
        return connection;
    }

    private DSLContext dsl() throws SQLException {
        return DSL.using(getConnection(), SQLDialect.POSTGRES);
    }

    /**
     * List all edge tables (tables named edges_*).
     */
    public Set<String> listEdgeTables() {
        Set<String> tables = new LinkedHashSet<>();
        try {
            Connection conn = getConnection();
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getTables(null, "public", "edges_%", new String[]{"TABLE"})) {
                while (rs.next()) {
                    tables.add(rs.getString("TABLE_NAME"));
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to list edge tables", e);
            throw new RuntimeException(e);
        }
        return tables;
    }

    /**
     * List all node tables (tables named nodes_*).
     */
    public Set<String> listNodeTables() {
        Set<String> tables = new LinkedHashSet<>();
        try {
            Connection conn = getConnection();
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getTables(null, "public", "nodes_%", new String[]{"TABLE"})) {
                while (rs.next()) {
                    tables.add(rs.getString("TABLE_NAME"));
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to list node tables", e);
            throw new RuntimeException(e);
        }
        return tables;
    }

    /**
     * Get graph names from edge table names (edges_{graph} -> graph).
     */
    public Set<String> getGraphs() {
        Set<String> graphs = new LinkedHashSet<>();
        for (String table : listEdgeTables()) {
            if (table.startsWith("edges_")) {
                graphs.add(table.substring("edges_".length()));
            }
        }
        return graphs;
    }

    private Table<?> edgesTable(String graph) {
        if (!graph.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException("Invalid graph name");
        }
        return table(name("edges_" + graph));
    }

    private Field<String> checkedColumn(String columnName) {
        if (!ALLOWED_COLUMNS.contains(columnName)) {
            throw new IllegalArgumentException("Disallowed column: " + columnName);
        }
        return field(name(columnName), String.class);
    }

    private List<Condition> buildConditions(String filterField, String filterValue,
                                             Map<String, List<String>> extraFilters) {
        var conditions = new ArrayList<Condition>();
        conditions.add(checkedColumn(filterField).eq(filterValue));
        if (extraFilters != null) {
            for (var entry : extraFilters.entrySet()) {
                String key = entry.getKey();
                var values = entry.getValue();
                if (values == null || values.isEmpty()) continue;

                // Handle negative array filters like -grebi:datasources
                if (key.startsWith("-")) {
                    String arrayCol = key.substring(1);
                    if (!ALLOWED_ARRAY_COLUMNS.contains(arrayCol)) continue;
                    for (String val : values) {
                        // NOT (col @> ARRAY[val]) — exclude rows containing this value
                        conditions.add(
                            condition("NOT ({0} @> ARRAY[{1}]::text[])",
                                field(name(arrayCol)), inline(val))
                        );
                    }
                } else if (ALLOWED_COLUMNS.contains(key)) {
                    conditions.add(checkedColumn(key).eq(values.get(0)));
                }
            }
        }
        return conditions;
    }

    private List<OrderField<?>> buildOrderBy(String sortField, String sortDir) {
        if (sortField == null) return List.of();
        var col = checkedColumn(sortField);
        return List.of("asc".equalsIgnoreCase(sortDir) ? col.asc() : col.desc());
    }

    /**
     * Build conditions from a filters map only (no required filter field).
     * Supports:
     *   - grebi:type=value (exact column match)
     *   - grebi:datasources=value (array contains)
     *   - -grebi:datasources=value (array excludes)
     */
    private List<Condition> buildConditionsFromFilters(Map<String, List<String>> filters) {
        var conditions = new ArrayList<Condition>();
        if (filters != null) {
            for (var entry : filters.entrySet()) {
                String key = entry.getKey();
                var values = entry.getValue();
                if (values == null || values.isEmpty()) continue;

                if (key.startsWith("-")) {
                    String arrayCol = key.substring(1);
                    if (!ALLOWED_ARRAY_COLUMNS.contains(arrayCol)) continue;
                    for (String val : values) {
                        conditions.add(
                            condition("NOT ({0} @> ARRAY[{1}]::text[])",
                                field(name(arrayCol)), inline(val))
                        );
                    }
                } else if (ALLOWED_ARRAY_CONTAINS_COLUMNS.contains(key)) {
                    for (String val : values) {
                        conditions.add(
                            condition("{0} @> ARRAY[{1}]::text[]",
                                field(name(key)), inline(val))
                        );
                    }
                } else if (ALLOWED_COLUMNS.contains(key)) {
                    conditions.add(checkedColumn(key).eq(values.get(0)));
                }
            }
        }
        return conditions;
    }

    /**
     * Search edges with optional filters (no required node ID). Returns full edge rows.
     * When unfiltered, uses estimated count from pg_class and skips facets for performance.
     */
    public EdgeQueryResult searchEdges(String graph,
                                        Map<String, List<String>> filters,
                                        String sortField, String sortDir,
                                        int offset, int limit) {
        try {
            var ctx = dsl();
            var tbl = edgesTable(graph);
            var conditions = buildConditionsFromFilters(filters);
            boolean unfiltered = conditions.isEmpty();

            // Fast estimated count — avoids COUNT(*) full scan
            long totalCount = estimateRowCount(ctx, tbl, conditions, graph, unfiltered);
            boolean cheapFacets = totalCount < 100_000;

            var rowJson = field("row_to_json({0})", String.class, tbl);
            List<Map<String, Object>> results = new ArrayList<>();
            for (var record : ctx.select(rowJson)
                    .from(tbl)
                    .where(conditions)
                    .orderBy(buildOrderBy(sortField, sortDir))
                    .limit(limit)
                    .offset(offset)
                    .fetch()) {
                results.add(gson.fromJson(record.value1(),
                        new TypeToken<Map<String, Object>>() {}.getType()));
            }

            Map<String, Map<String, Long>> facets;
            if (cheapFacets && !unfiltered) {
                facets = computeFacets(ctx, tbl, conditions, EDGE_FACET_FIELDS);
                var typeField = field(name("grebi:type"), String.class);
                var cnt = count().as("cnt");
                Map<String, Long> typeCounts = new LinkedHashMap<>();
                for (var record : ctx.select(typeField, cnt)
                        .from(tbl)
                        .where(conditions)
                        .groupBy(typeField)
                        .orderBy(cnt.desc())
                        .fetch()) {
                    typeCounts.put(record.get(typeField), record.get(cnt).longValue());
                }
                facets.put("grebi:type", typeCounts);
            } else {
                // Too many rows — skip expensive facet queries; frontend uses /stats endpoint
                facets = new LinkedHashMap<>();
            }

            return new EdgeQueryResult(results, totalCount, facets);
        } catch (SQLException e) {
            logger.error("Edge search failed", e);
            throw new RuntimeException(e);
        }
    }

    private long estimateRowCount(DSLContext ctx, Table<?> tbl, List<Condition> conditions,
                                   String graph, boolean unfiltered) {
        if (unfiltered) {
            var tableName = "edges_" + graph;
            return ctx.select(field("reltuples::bigint", Long.class))
                    .from(table("pg_class"))
                    .where(field("relname").eq(tableName))
                    .fetchOptional()
                    .map(r -> r.value1())
                    .orElse(0L);
        }
        try {
            var query = ctx.selectOne().from(tbl).where(conditions);
            String sql = query.getSQL(ParamType.INLINED);
            var result = ctx.fetchOne("EXPLAIN (FORMAT JSON) " + sql);
            String json = result.get(0, String.class);
            var arr = gson.fromJson(json, JsonArray.class);
            return arr.get(0).getAsJsonObject()
                    .getAsJsonObject("Plan")
                    .get("Plan Rows").getAsLong();
        } catch (Exception e) {
            logger.warn("EXPLAIN estimate failed, falling back to COUNT", e);
            return ctx.select(count()).from(tbl).where(conditions).fetchSingle().value1();
        }
    }

    /**
     * Query edges with pagination and filtering.
     */
    public EdgeQueryResult queryEdges(String graph, String filterField, String filterValue,
                                       Map<String, List<String>> extraFilters,
                                       String sortField, String sortDir,
                                       int offset, int limit) {
        try {
            var ctx = dsl();
            var tbl = edgesTable(graph);
            var conditions = buildConditions(filterField, filterValue, extraFilters);

            long totalCount = ctx.select(count())
                    .from(tbl)
                    .where(conditions)
                    .fetchSingle()
                    .value1();

            var rowJson = field("row_to_json({0})", String.class, tbl);
            List<Map<String, Object>> results = new ArrayList<>();
            for (var record : ctx.select(rowJson)
                    .from(tbl)
                    .where(conditions)
                    .orderBy(buildOrderBy(sortField, sortDir))
                    .limit(limit)
                    .offset(offset)
                    .fetch()) {
                results.add(gson.fromJson(record.value1(),
                        new TypeToken<Map<String, Object>>() {}.getType()));
            }

            var facets = computeFacets(ctx, tbl, conditions, EDGE_FACET_FIELDS);

            return new EdgeQueryResult(results, totalCount, facets);
        } catch (SQLException e) {
            logger.error("Edge query failed", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Query edge refs (lightweight: only type, datasources, fromNodeId, toNodeId).
     */
    public EdgeQueryResult queryEdgeRefs(String graph, String filterField, String filterValue,
                                          Map<String, List<String>> extraFilters,
                                          String sortField, String sortDir,
                                          int offset, int limit) {
        try {
            var ctx = dsl();
            var tbl = edgesTable(graph);
            var conditions = buildConditions(filterField, filterValue, extraFilters);
            var datasources = field(name("grebi:datasources"));

            long totalCount = ctx.select(count())
                    .from(tbl)
                    .where(conditions)
                    .fetchSingle()
                    .value1();

            List<Map<String, Object>> results = new ArrayList<>();
            for (var record : ctx.select(GREBI_TYPE, datasources, GREBI_FROM_NODE_ID, GREBI_TO_NODE_ID, GREBI_REFS)
                    .from(tbl)
                    .where(conditions)
                    .orderBy(buildOrderBy(sortField, sortDir))
                    .limit(limit)
                    .offset(offset)
                    .fetch()) {
                Map<String, Object> ref = new LinkedHashMap<>();
                ref.put("grebi:type", record.get(GREBI_TYPE));
                ref.put("grebi:datasources", toDatasourceList(record.get(datasources)));
                ref.put("grebi:fromNodeId", record.get(GREBI_FROM_NODE_ID));
                ref.put("grebi:toNodeId", record.get(GREBI_TO_NODE_ID));
                String refsJson = record.get(GREBI_REFS);
                if (refsJson != null) {
                    ref.put("_refs", gson.fromJson(refsJson,
                            new com.google.gson.reflect.TypeToken<Map<String, Object>>() {}.getType()));
                }
                results.add(ref);
            }

            var facets = computeFacets(ctx, tbl, conditions, EDGE_FACET_FIELDS);

            return new EdgeQueryResult(results, totalCount, facets);
        } catch (SQLException e) {
            logger.error("Edge ref query failed", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Get edge counts grouped by type and datasource.
     */
    public Map<String, Map<String, Integer>> getEdgeCounts(String graph, String filterField, String filterValue) {
        try {
            var ctx = dsl();
            var tbl = edgesTable(graph);
            var dsField = field(name("ds"), String.class);
            var cnt = count().as("cnt");
            // jOOQ plain SQL template — {0} is rendered as a quoted identifier by jOOQ, not concatenated
            var unnested = table("unnest({0}) as ds", field(name("grebi:datasources")));

            Map<String, Map<String, Integer>> result = new LinkedHashMap<>();
            for (var record : ctx.select(GREBI_TYPE, dsField, cnt)
                    .from(tbl, unnested)
                    .where(checkedColumn(filterField).eq(filterValue))
                    .groupBy(GREBI_TYPE, dsField)
                    .fetch()) {
                result.computeIfAbsent(record.get(GREBI_TYPE), k -> new LinkedHashMap<>())
                        .put(record.get(dsField), record.get(cnt));
            }
            return result;
        } catch (SQLException e) {
            logger.error("Edge count query failed", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Get a single edge by its ID.
     */
    public Map<String, Object> getEdgeById(String graph, String edgeId) {
        try {
            var ctx = dsl();
            var tbl = edgesTable(graph);
            var rowJson = field("row_to_json({0})", String.class, tbl);
            var record = ctx.select(rowJson)
                    .from(tbl)
                    .where(field(name("grebi:edgeId"), String.class).eq(edgeId))
                    .fetchOne();

            if (record != null) {
                return gson.fromJson(record.value1(),
                        new TypeToken<Map<String, Object>>() {}.getType());
            }
        } catch (SQLException e) {
            logger.error("Get edge by ID failed", e);
            throw new RuntimeException(e);
        }
        return null;
    }

    private static List<String> toDatasourceList(Object raw) {
        if (raw instanceof String[] arr) return Arrays.asList(arr);
        if (raw instanceof Object[] arr) {
            List<String> list = new ArrayList<>(arr.length);
            for (Object o : arr) list.add(String.valueOf(o));
            return list;
        }
        return List.of();
    }

    /**
     * Compute facet counts for array columns (e.g. grebi:datasources) using unnest + group by.
     */
    private Map<String, Map<String, Long>> computeFacets(DSLContext ctx, Table<?> tbl,
                                                          List<Condition> conditions,
                                                          List<String> facetFields) {
        return computeFacets(ctx, tbl, conditions, facetFields, ALLOWED_ARRAY_COLUMNS);
    }

    private Map<String, Map<String, Long>> computeFacets(DSLContext ctx, Table<?> tbl,
                                                          List<Condition> conditions,
                                                          List<String> facetFields,
                                                          Set<String> allowedArrayCols) {
        Map<String, Map<String, Long>> facets = new LinkedHashMap<>();
        if (facetFields == null || facetFields.isEmpty()) return facets;

        for (String facetField : facetFields) {
            if (!allowedArrayCols.contains(facetField)) continue;

            var valField = field(name("_facet_val"), String.class);
            var cnt = count().as("cnt");
            var unnested = table("unnest({0}) as {1}",
                    field(name(facetField)), name("_facet_val"));

            Map<String, Long> counts = new LinkedHashMap<>();
            for (var record : ctx.select(valField, cnt)
                    .from(tbl, unnested)
                    .where(conditions)
                    .groupBy(valField)
                    .orderBy(cnt.desc())
                    .fetch()) {
                counts.put(record.get(valField), record.get(cnt).longValue());
            }
            facets.put(facetField, counts);
        }
        return facets;
    }

    public static class EdgeQueryResult {
        public final List<Map<String, Object>> results;
        public final long totalCount;
        public final Map<String, Map<String, Long>> facets;

        public EdgeQueryResult(List<Map<String, Object>> results, long totalCount,
                               Map<String, Map<String, Long>> facets) {
            this.results = results;
            this.totalCount = totalCount;
            this.facets = facets;
        }
    }

    private Table<?> nodesTable(String graph) {
        if (!graph.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException("Invalid graph name");
        }
        return table(name("nodes_" + graph));
    }

    private static final Pattern EMBEDDING_COL_PATTERN = Pattern.compile("^embedding:[a-zA-Z0-9_-]+$");

    private String checkedEmbeddingColumn(String embeddingModel) {
        String col = "embedding:" + embeddingModel;
        if (!EMBEDDING_COL_PATTERN.matcher(col).matches()) {
            throw new IllegalArgumentException("Invalid embedding model name: " + embeddingModel);
        }
        return col;
    }

    /**
     * Search nodes by vector similarity using pgvector cosine distance.
     * Returns nodeIds and distances, ordered by distance ascending.
     */
    public List<VectorSearchResult> searchByVector(String graph, String embeddingModel,
                                                    float[] queryVector, int limit) {
        String col = checkedEmbeddingColumn(embeddingModel);

        // Build the pgvector literal: [0.1,0.2,...]
        StringBuilder vecLiteral = new StringBuilder("[");
        for (int i = 0; i < queryVector.length; i++) {
            if (i > 0) vecLiteral.append(",");
            vecLiteral.append(queryVector[i]);
        }
        vecLiteral.append("]");

        try {
            Connection conn = getConnection();
            var tbl = nodesTable(graph);
            var nodeIdField = field(name("grebi:nodeId"), String.class);
            var nameField = field(name("grebi:name"), String.class);

            // Use raw SQL for the cosine distance operator since jOOQ doesn't natively support <=>
            String sql = "SELECT \"grebi:nodeId\", \"grebi:name\", " +
                    "\"grebi:datasources\", \"grebi:type\", \"grebi:sourceIds\", " +
                    "\"" + col + "\" <=> ?::vector AS distance " +
                    "FROM " + dsl().render(tbl) + " " +
                    "WHERE \"" + col + "\" IS NOT NULL " +
                    "ORDER BY distance " +
                    "LIMIT ?";

            List<VectorSearchResult> results = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, vecLiteral.toString());
                ps.setInt(2, limit);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        java.sql.Array dsArr = rs.getArray("grebi:datasources");
                        java.sql.Array typeArr = rs.getArray("grebi:type");
                        java.sql.Array srcArr = rs.getArray("grebi:sourceIds");
                        results.add(new VectorSearchResult(
                                rs.getString("grebi:nodeId"),
                                rs.getString("grebi:name"),
                                dsArr != null ? java.util.Arrays.asList((String[]) dsArr.getArray()) : List.of(),
                                typeArr != null ? java.util.Arrays.asList((String[]) typeArr.getArray()) : List.of(),
                                srcArr != null ? java.util.Arrays.asList((String[]) srcArr.getArray()) : List.of(),
                                rs.getDouble("distance")
                        ));
                    }
                }
            }
            return results;
        } catch (SQLException e) {
            logger.error("Vector search failed", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Get a node's embedding vector for a given model.
     */
    public float[] getNodeEmbedding(String graph, String nodeId, String embeddingModel) {
        String col = checkedEmbeddingColumn(embeddingModel);

        try {
            Connection conn = getConnection();
            var tbl = nodesTable(graph);

            String sql = "SELECT \"" + col + "\"::text FROM " + dsl().render(tbl) +
                    " WHERE \"grebi:nodeId\" = ?";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, nodeId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        String vecStr = rs.getString(1);
                        if (vecStr == null) return null;
                        return parseVectorLiteral(vecStr);
                    }
                }
            }
            return null;
        } catch (SQLException e) {
            logger.error("Get node embedding failed", e);
            throw new RuntimeException(e);
        }
    }

    private static float[] parseVectorLiteral(String literal) {
        // pgvector returns "[0.1,0.2,...]"
        String inner = literal.substring(1, literal.length() - 1);
        String[] parts = inner.split(",");
        float[] result = new float[parts.length];
        for (int i = 0; i < parts.length; i++) {
            result[i] = Float.parseFloat(parts[i]);
        }
        return result;
    }

    public static class VectorSearchResult {
        public final String nodeId;
        public final String name;
        public final List<String> datasources;
        public final List<String> type;
        public final List<String> sourceIds;
        public final double distance;

        public VectorSearchResult(String nodeId, String name, List<String> datasources, List<String> type, List<String> sourceIds, double distance) {
            this.nodeId = nodeId;
            this.name = name;
            this.datasources = datasources;
            this.type = type;
            this.sourceIds = sourceIds;
            this.distance = distance;
        }
    }

    /**
     * List all blob tables (tables named blobs_*).
     */
    public Set<String> listBlobTables() {
        Set<String> tables = new LinkedHashSet<>();
        try {
            Connection conn = getConnection();
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getTables(null, "public", "blobs_%", new String[]{"TABLE"})) {
                while (rs.next()) {
                    tables.add(rs.getString("TABLE_NAME"));
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to list blob tables", e);
            throw new RuntimeException(e);
        }
        return tables;
    }

    /**
     * Get graph names from blob table names (blobs_{graph} -> graph).
     */
    public Set<String> getBlobGraphs() {
        Set<String> graphs = new LinkedHashSet<>();
        for (String table : listBlobTables()) {
            if (table.startsWith("blobs_")) {
                graphs.add(table.substring("blobs_".length()));
            }
        }
        return graphs;
    }

    /**
     * Resolve IDs to their decompressed JSON blobs from the blobs table.
     * Returns a map of id -> parsed JSON object.
     */
    public Map<String, Map<String, Object>> resolveToMap(String graph, Collection<String> ids) {
        if (ids == null || ids.isEmpty()) return Map.of();

        if (!graph.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException("Invalid graph name");
        }

        Map<String, Map<String, Object>> result = new LinkedHashMap<>();
        String sql = "SELECT id, json FROM \"blobs_" + graph + "\" WHERE id = ANY(?)";

        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            String[] idArray = ids.toArray(new String[0]);
            byte[][] byteIds = new byte[idArray.length][];
            for (int i = 0; i < idArray.length; i++) {
                byteIds[i] = idArray[i].getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
            ps.setArray(1, getConnection().createArrayOf("bytea", byteIds));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    byte[] idBytes = rs.getBytes("id");
                    byte[] jsonBytes = rs.getBytes("json");
                    String id = new String(idBytes, java.nio.charset.StandardCharsets.UTF_8);
                    try (var is = new InflaterInputStream(new ByteArrayInputStream(jsonBytes));
                         var reader = new InputStreamReader(is, java.nio.charset.StandardCharsets.UTF_8)) {
                        Map<String, Object> parsed = gson.fromJson(reader,
                                new TypeToken<Map<String, Object>>() {}.getType());
                        result.put(id, parsed);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Blob resolve failed for graph {}", graph, e);
            throw new RuntimeException(e);
        }
        return result;
    }

    /**
     * Resolve IDs to their decompressed JSON blobs, returned as a list in the same order as the input.
     */
    public List<Map<String, Object>> resolveToList(String graph, Collection<String> ids) {
        var resolved = resolveToMap(graph, ids);
        List<Map<String, Object>> result = new ArrayList<>();
        for (String id : ids) {
            var val = resolved.get(id);
            if (val == null) {
                logger.warn("Could not resolve id {} in graph {}", id, graph);
            }
            result.add(val);
        }
        return result;
    }

    // --- Autocomplete ---

    private Table<?> autocompleteTable(String graph) {
        if (!graph.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException("Invalid graph name");
        }
        return table(name("autocomplete_" + graph));
    }

    public Set<String> listAutocompleteTables() {
        Set<String> tables = new LinkedHashSet<>();
        try {
            Connection conn = getConnection();
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getTables(null, "public", "autocomplete_%", new String[]{"TABLE"})) {
                while (rs.next()) {
                    tables.add(rs.getString("TABLE_NAME"));
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to list autocomplete tables", e);
            throw new RuntimeException(e);
        }
        return tables;
    }

    public List<String> autocomplete(String graph, String q) {
        if (q == null || q.isBlank()) return List.of();
        try {
            var ctx = dsl();
            var tbl = autocompleteTable(graph);
            var labelField = field(name("label"), String.class);
            return ctx.selectDistinct(labelField)
                    .from(tbl)
                    .where(labelField.likeIgnoreCase("%" + escapeLike(q) + "%"))
                    .orderBy(field("similarity({0}, {1})", Double.class, labelField, val(q)).desc())
                    .limit(10)
                    .fetch(labelField);
        } catch (SQLException e) {
            logger.error("Autocomplete failed", e);
            throw new RuntimeException(e);
        }
    }

    // --- Node search ---

    private List<Condition> buildNodeConditions(Map<String, List<String>> filters) {
        var conditions = new ArrayList<Condition>();
        if (filters != null) {
            for (var entry : filters.entrySet()) {
                String key = entry.getKey();
                var values = entry.getValue();
                if (values == null || values.isEmpty()) continue;

                if (key.startsWith("-")) {
                    String arrayCol = key.substring(1);
                    if (!ALLOWED_NODE_ARRAY_COLUMNS.contains(arrayCol)) continue;
                    for (String val : values) {
                        conditions.add(
                            condition("NOT ({0} @> ARRAY[{1}]::text[])",
                                field(name(arrayCol)), inline(val))
                        );
                    }
                } else if (ALLOWED_NODE_ARRAY_COLUMNS.contains(key)) {
                    for (String val : values) {
                        conditions.add(
                            condition("{0} @> ARRAY[{1}]::text[]",
                                field(name(key)), inline(val))
                        );
                    }
                } else if (ALLOWED_NODE_COLUMNS.contains(key)) {
                    conditions.add(field(name(key), String.class).eq(values.get(0)));
                }
            }
        }
        return conditions;
    }

    public NodeQueryResult searchNodes(String graph, String q,
                                        Map<String, List<String>> filters,
                                        int offset, int limit) {
        try {
            var ctx = dsl();
            var tbl = nodesTable(graph);
            var conditions = buildNodeConditions(filters);
            var nameField = field(name("grebi:name"), String.class);

            if (q != null && !q.isBlank()) {
                conditions.add(nameField.likeIgnoreCase("%" + escapeLike(q) + "%"));
            }

            boolean unfiltered = conditions.isEmpty();
            long totalCount;
            if (unfiltered) {
                totalCount = ctx.select(field("reltuples::bigint", Long.class))
                        .from(table("pg_class"))
                        .where(field("relname").eq("nodes_" + graph))
                        .fetchOptional()
                        .map(r -> r.value1())
                        .orElse(0L);
            } else {
                try {
                    var countQuery = ctx.selectOne().from(tbl).where(conditions);
                    String sql = countQuery.getSQL(ParamType.INLINED);
                    var result = ctx.fetchOne("EXPLAIN (FORMAT JSON) " + sql);
                    String json = result.get(0, String.class);
                    var arr = gson.fromJson(json, JsonArray.class);
                    totalCount = arr.get(0).getAsJsonObject()
                            .getAsJsonObject("Plan")
                            .get("Plan Rows").getAsLong();
                } catch (Exception e) {
                    logger.warn("EXPLAIN estimate failed for nodes, falling back to COUNT", e);
                    totalCount = ctx.select(count()).from(tbl).where(conditions).fetchSingle().value1();
                }
            }

            var nodeIdField = field(name("grebi:nodeId"), String.class);
            var typeField = field(name("grebi:type"));
            var dsField = field(name("grebi:datasources"));
            var srcField = field(name("grebi:sourceIds"));
            var curieField = field(name("ols:curie"), String.class);

            var select = ctx.select(nodeIdField, nameField, typeField, dsField, srcField, curieField)
                    .from(tbl)
                    .where(conditions);

            List<OrderField<?>> orderBy;
            if (q != null && !q.isBlank()) {
                orderBy = List.of(
                    field("similarity({0}, {1})", Double.class, nameField, val(q)).desc()
                );
            } else {
                orderBy = List.of(nameField.asc());
            }

            List<Map<String, Object>> results = new ArrayList<>();
            for (var record : select.orderBy(orderBy).limit(limit).offset(offset).fetch()) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("grebi:nodeId", record.get(nodeIdField));
                row.put("grebi:name", record.get(nameField));
                row.put("grebi:type", toDatasourceList(record.get(typeField)));
                row.put("grebi:datasources", toDatasourceList(record.get(dsField)));
                row.put("grebi:sourceIds", toDatasourceList(record.get(srcField)));
                row.put("ols:curie", record.get(curieField));
                results.add(row);
            }

            Map<String, Map<String, Long>> facets = new LinkedHashMap<>();
            boolean cheapFacets = totalCount < 100_000;
            if (cheapFacets && !unfiltered) {
                facets = computeFacets(ctx, tbl, conditions, NODE_FACET_FIELDS, ALLOWED_NODE_ARRAY_COLUMNS);
            }

            return new NodeQueryResult(results, totalCount, facets);
        } catch (SQLException e) {
            logger.error("Node search failed", e);
            throw new RuntimeException(e);
        }
    }

    public static class NodeQueryResult {
        public final List<Map<String, Object>> results;
        public final long totalCount;
        public final Map<String, Map<String, Long>> facets;

        public NodeQueryResult(List<Map<String, Object>> results, long totalCount,
                               Map<String, Map<String, Long>> facets) {
            this.results = results;
            this.totalCount = totalCount;
            this.facets = facets;
        }
    }

    // --- Materialised queries ---

    private Table<?> matQueryTable(String graph) {
        if (!graph.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException("Invalid graph name");
        }
        return table(name("materialised_queries_" + graph));
    }

    public Set<String> listMatQueryTables() {
        Set<String> tables = new LinkedHashSet<>();
        try {
            Connection conn = getConnection();
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getTables(null, "public", "materialised_queries_%", new String[]{"TABLE"})) {
                while (rs.next()) {
                    tables.add(rs.getString("TABLE_NAME"));
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to list materialised query tables", e);
            throw new RuntimeException(e);
        }
        return tables;
    }

    public MatQueryResult searchMaterialisedQueryResults(
            String graph, String queryId, String searchText,
            Map<String, List<String>> filters, List<String> facetFields,
            int offset, int limit) {
        try {
            var ctx = dsl();
            var tbl = matQueryTable(graph);
            var queryIdField = field(name("query_id"), String.class);
            var rowNumField = field(name("row_number"), Integer.class);
            var dataField = field(name("data"), String.class);

            var conditions = new ArrayList<Condition>();
            conditions.add(queryIdField.eq(queryId));

            if (searchText != null && !searchText.isBlank()) {
                conditions.add(
                    condition("({0})::text ILIKE {1}",
                        field(name("data")), val("%" + escapeLike(searchText) + "%"))
                );
            }

            if (filters != null) {
                for (var entry : filters.entrySet()) {
                    String key = entry.getKey();
                    var values = entry.getValue();
                    if (values == null || values.isEmpty()) continue;
                    for (String v : values) {
                        conditions.add(
                            condition("{0} ->> {1} = {2}",
                                field(name("data")), val(key), val(v))
                        );
                    }
                }
            }

            long totalCount = ctx.select(count())
                    .from(tbl)
                    .where(conditions)
                    .fetchSingle()
                    .value1();

            List<Map<String, Object>> results = new ArrayList<>();
            for (var record : ctx.select(dataField)
                    .from(tbl)
                    .where(conditions)
                    .orderBy(rowNumField.asc())
                    .limit(limit)
                    .offset(offset)
                    .fetch()) {
                results.add(gson.fromJson(record.value1(),
                        new TypeToken<Map<String, Object>>() {}.getType()));
            }

            Map<String, Map<String, Long>> facets = new LinkedHashMap<>();
            if (facetFields != null && !facetFields.isEmpty() && totalCount < 100_000) {
                for (String facetField : facetFields) {
                    var extracted = field("{0} ->> {1}", String.class,
                            field(name("data")), val(facetField));
                    var cnt = count().as("cnt");
                    Map<String, Long> counts = new LinkedHashMap<>();
                    for (var record : ctx.select(extracted, cnt)
                            .from(tbl)
                            .where(conditions)
                            .groupBy(extracted)
                            .orderBy(cnt.desc())
                            .fetch()) {
                        String fval = record.get(extracted);
                        if (fval != null) {
                            counts.put(fval, record.get(cnt).longValue());
                        }
                    }
                    facets.put(facetField, counts);
                }
            }

            return new MatQueryResult(results, totalCount, facets);
        } catch (SQLException e) {
            logger.error("Materialised query search failed", e);
            throw new RuntimeException(e);
        }
    }

    public static class MatQueryResult {
        public final List<Map<String, Object>> results;
        public final long totalCount;
        public final Map<String, Map<String, Long>> facets;

        public MatQueryResult(List<Map<String, Object>> results, long totalCount,
                              Map<String, Map<String, Long>> facets) {
            this.results = results;
            this.totalCount = totalCount;
            this.facets = facets;
        }
    }

    private static String escapeLike(String s) {
        return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }
}
