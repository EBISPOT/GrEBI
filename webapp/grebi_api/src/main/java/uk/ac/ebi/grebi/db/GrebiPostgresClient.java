package uk.ac.ebi.grebi.db;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.reflect.TypeToken;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import uk.ac.ebi.grebi.ResourceLimits;
import org.jooq.*;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.io.ByteArrayInputStream;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;
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
            "grebi:nodeId", "grebi:name", "ols:curie", "grebi:curie"
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
    private final String password;
    private final String sslMode;
    private final String jdbcParams;
    private volatile HikariDataSource dataSource;

    public GrebiPostgresClient() {
        this.host = getEnvOrDefault("GREBI_POSTGRES_HOST", "localhost");
        this.port = getEnvOrDefault("GREBI_POSTGRES_PORT", "5432");
        this.user = getEnvOrDefault("GREBI_POSTGRES_USER", "grebi");
        this.dbName = getEnvOrDefault("GREBI_POSTGRES_DB", "grebi");
        this.password = System.getenv("GREBI_POSTGRES_PASSWORD");
        this.sslMode = System.getenv("GREBI_POSTGRES_SSLMODE");
        this.jdbcParams = System.getenv("GREBI_POSTGRES_JDBC_PARAMS");

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

    private static String getSchemaPattern(Connection conn) {
        try {
            String schema = conn.getSchema();
            if (schema != null && !schema.isBlank()) {
                return schema;
            }
        } catch (SQLException ignored) {
            // Fall back to all visible schemas below.
        }
        return null;
    }

    private static Set<String> listTables(DatabaseMetaData meta, String schemaPattern, String tablePattern)
            throws SQLException {
        Set<String> tables = new LinkedHashSet<>();
        try (ResultSet rs = meta.getTables(null, schemaPattern, tablePattern, new String[]{"TABLE"})) {
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }
        }
        return tables;
    }

    private String getJdbcBaseUrl() {
        return "jdbc:postgresql://" + host + ":" + port + "/" + dbName;
    }

    private String getJdbcUrl() {
        String baseUrl = getJdbcBaseUrl();
        var params = Stream.of(
                normaliseJdbcParam("sslmode", sslMode),
                normaliseRawJdbcParams(jdbcParams)
        )
            .filter(param -> !param.isBlank())
            .collect(Collectors.joining("&"));

        return params.isBlank() ? baseUrl : baseUrl + "?" + params;
    }

    private static String normaliseJdbcParam(String key, String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        return key + "=" + value;
    }

    private static String normaliseRawJdbcParams(String value) {
        if (value == null) {
            return "";
        }
        return value.strip().replaceFirst("^[?&]+", "");
    }

    private HikariDataSource getDataSource() {
        var ds = dataSource;
        if (ds != null) {
            return ds;
        }
        synchronized (this) {
            ds = dataSource;
            if (ds != null) {
                return ds;
            }
            logger.info("Connecting to PostgreSQL at {}", getJdbcBaseUrl());
            var config = new HikariConfig();
            config.setJdbcUrl(getJdbcUrl());
            config.setUsername(user);
            if (password != null && !password.isBlank()) {
                config.setPassword(password);
            }
            config.setMaximumPoolSize(16);
            config.setMinimumIdle(0);
            config.setAutoCommit(true);
            config.setConnectionInitSql("SET statement_timeout TO " + ResourceLimits.get().queryTimeoutMillis());
            config.setPoolName("grebi-postgres");
            config.setInitializationFailTimeout(-1);
            config.setConnectionTimeout(30_000);
            config.setValidationTimeout(5_000);
            config.setKeepaliveTime(120_000);
            ds = new HikariDataSource(config);
            dataSource = ds;
            return ds;
        }
    }

    public Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    private DSLContext dsl() throws SQLException {
        return DSL.using(getDataSource(), SQLDialect.POSTGRES);
    }

    /**
     * Load all graph metadata from the graph_metadata table.
     * Returns a map of graph name → metadata JSON.
     */
    public Map<String, JsonElement> getGraphMetadata() {
        Map<String, JsonElement> result = new LinkedHashMap<>();
        try (Connection conn = getConnection()) {
            try (java.sql.Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT graph, metadata FROM graph_metadata")) {
                while (rs.next()) {
                    String graph = rs.getString("graph");
                    String json = rs.getString("metadata");
                    result.put(graph, JsonParser.parseString(json));
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to load graph metadata from PostgreSQL", e);
            throw new RuntimeException(e);
        }
        return result;
    }

    /**
     * List all edge tables (tables named edges_*).
     */
    public Set<String> listEdgeTables() {
        Set<String> tables = new LinkedHashSet<>();
        try (Connection conn = getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            String schemaPattern = getSchemaPattern(conn);
            tables.addAll(listTables(meta, schemaPattern, "edges_%"));
            if (tables.isEmpty() && schemaPattern != null) {
                tables.addAll(listTables(meta, null, "edges_%"));
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
        try (Connection conn = getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            String schemaPattern = getSchemaPattern(conn);
            tables.addAll(listTables(meta, schemaPattern, "nodes_%"));
            if (tables.isEmpty() && schemaPattern != null) {
                tables.addAll(listTables(meta, null, "nodes_%"));
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
        if (raw instanceof java.sql.Array sqlArr) {
            try {
                Object unwrapped = sqlArr.getArray();
                if (unwrapped instanceof String[] sArr) return Arrays.asList(sArr);
                if (unwrapped instanceof Object[] oArr) {
                    List<String> list = new ArrayList<>(oArr.length);
                    for (Object o : oArr) list.add(String.valueOf(o));
                    return list;
                }
            } catch (java.sql.SQLException e) {
                logger.warn("Failed to unwrap SQL array", e);
            }
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

        try (Connection conn = getConnection()) {
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

        try (Connection conn = getConnection()) {
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
        try (Connection conn = getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            String schemaPattern = getSchemaPattern(conn);
            tables.addAll(listTables(meta, schemaPattern, "blobs_%"));
            if (tables.isEmpty() && schemaPattern != null) {
                tables.addAll(listTables(meta, null, "blobs_%"));
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

        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            String[] idArray = ids.toArray(new String[0]);
            byte[][] byteIds = new byte[idArray.length][];
            for (int i = 0; i < idArray.length; i++) {
                byteIds[i] = idArray[i].getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
            ps.setArray(1, conn.createArrayOf("bytea", byteIds));
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
        try (Connection conn = getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            String schemaPattern = getSchemaPattern(conn);
            tables.addAll(listTables(meta, schemaPattern, "autocomplete_%"));
            if (tables.isEmpty() && schemaPattern != null) {
                tables.addAll(listTables(meta, null, "autocomplete_%"));
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
            var sim = field("similarity({0}, {1})", Double.class, labelField, val(q));
            return ctx.select(labelField)
                    .from(tbl)
                    .where(labelField.likeIgnoreCase("%" + escapeLike(q) + "%"))
                    .groupBy(labelField)
                    .orderBy(sim.desc())
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

    private static boolean hasSingleFilterValue(Map<String, List<String>> filters, String key) {
        if (filters == null) {
            return false;
        }
        var values = filters.get(key);
        return values != null && values.size() == 1 && values.get(0) != null && !values.get(0).isBlank();
    }

    private static boolean isExactIdentifierLookup(Map<String, List<String>> filters) {
        if (filters == null || filters.size() != 1) {
            return false;
        }
        return hasSingleFilterValue(filters, "grebi:sourceIds")
                || hasSingleFilterValue(filters, "grebi:nodeId")
                || hasSingleFilterValue(filters, "ols:curie")
                || hasSingleFilterValue(filters, "grebi:curie");
    }

    private List<Map<String, Object>> fetchExactIdentifierLookup(
            DSLContext ctx,
            SelectConditionStep<Record6<String, String, Object, Object, Object, String>> select,
            int offset,
            int limit) {
        var sql = "WITH filtered AS MATERIALIZED (" +
                select.getSQL(ParamType.INLINED) +
                ") SELECT * FROM filtered LIMIT " + limit + " OFFSET " + offset;

        List<Map<String, Object>> results = new ArrayList<>();
        for (var record : ctx.fetch(sql)) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("grebi:nodeId", record.get("grebi:nodeId", String.class));
            row.put("grebi:name", record.get("grebi:name", String.class));
            row.put("grebi:type", toDatasourceList(record.get("grebi:type")));
            row.put("grebi:datasources", toDatasourceList(record.get("grebi:datasources")));
            row.put("grebi:sourceIds", toDatasourceList(record.get("grebi:sourceIds")));
            row.put("grebi:curie", record.get("grebi:curie", String.class));
            results.add(row);
        }
        return results;
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
            // grebi:curie is derived for every node (the compact CURIE of its id,
            // or the id itself when no prefix matches), so it is the pipeline-wide
            // identifier that replaces the OLS-only ols:curie. Expose it directly.
            // (Requires graphs reloaded since the grebi:curie column was added.)
            var curieField = field(name("grebi:curie"), String.class);

            var select = ctx.select(nodeIdField, nameField, typeField, dsField, srcField, curieField)
                    .from(tbl)
                    .where(conditions);

            boolean exactIdentifierLookup = (q == null || q.isBlank()) && isExactIdentifierLookup(filters);

            List<Map<String, Object>> results;
            if (q != null && !q.isBlank()) {
                results = new ArrayList<>();
                for (var record : select.orderBy(
                        field("similarity({0}, {1})", Double.class, nameField, val(q)).desc()
                ).limit(limit).offset(offset).fetch()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("grebi:nodeId", record.get(nodeIdField));
                    row.put("grebi:name", record.get(nameField));
                    row.put("grebi:type", toDatasourceList(record.get(typeField)));
                    row.put("grebi:datasources", toDatasourceList(record.get(dsField)));
                    row.put("grebi:sourceIds", toDatasourceList(record.get(srcField)));
                    row.put("grebi:curie", record.get(curieField));
                    results.add(row);
                }
            } else if (exactIdentifierLookup) {
                // PostgreSQL can still choose a seq scan for `WHERE identifier = ... LIMIT 1`
                // if it predicts an early hit. Wrapping the exact-match subquery in a
                // MATERIALIZED CTE forces the identifier predicate to run first and reliably
                // uses the selective index for sourceIds/nodeId/curie lookups.
                results = fetchExactIdentifierLookup(ctx, select, offset, limit);
            } else {
                results = new ArrayList<>();
                for (var record : select.orderBy(nameField.asc()).limit(limit).offset(offset).fetch()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("grebi:nodeId", record.get(nodeIdField));
                    row.put("grebi:name", record.get(nameField));
                    row.put("grebi:type", toDatasourceList(record.get(typeField)));
                    row.put("grebi:datasources", toDatasourceList(record.get(dsField)));
                    row.put("grebi:sourceIds", toDatasourceList(record.get(srcField)));
                    row.put("grebi:curie", record.get(curieField));
                    results.add(row);
                }
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
    //
    // Each materialised query lives in its own typed table (matq_{sg}_{query});
    // the table name and column types come from the build's graph_metadata
    // entry (MaterialisedBuild), never re-derived here. GraphNodeId columns are
    // stored as "<col>_id" TEXT[] (closure filter target, GIN-indexed) plus
    // "<col>_name" TEXT; the exact JSON row served to clients sits in a
    // payload BYTEA column.

    /** `ident`, guaranteed safe to interpolate as a quoted SQL identifier. */
    private static String requireIdent(String ident) {
        if (ident == null || !ident.matches("[A-Za-z0-9_]+")) {
            throw new IllegalArgumentException("Invalid identifier: " + ident);
        }
        return ident;
    }

    /** The physical column holding a logical column's filter/sort/facet value. */
    private static String physicalColumn(MaterialisedBuild build, String columnId) {
        requireIdent(columnId);
        String type = build.columnType(columnId);
        if ("GraphNodeId".equals(type)) {
            return columnId + "_name";
        }
        return columnId;
    }

    /** Browse a standalone materialised query's table (the /tables UI). */
    public MatQueryResult searchMaterialisedQueryResults(
            MaterialisedBuild build, String searchText,
            Map<String, List<String>> filters, List<String> facetFields,
            int offset, int limit) {
        String tbl = "\"" + requireIdent(build.table) + "\"";
        try (Connection conn = getConnection()) {
            StringBuilder where = new StringBuilder("TRUE");
            List<Object> binds = new ArrayList<>();

            if (searchText != null && !searchText.isBlank()) {
                where.append(" AND convert_from(payload, 'UTF8') ILIKE ?");
                binds.add("%" + escapeLike(searchText) + "%");
            }

            if (filters != null) {
                for (var entry : filters.entrySet()) {
                    var values = entry.getValue();
                    if (values == null || values.isEmpty()) continue;
                    String col = entry.getKey();
                    String type = build.columnType(col);
                    for (String v : values) {
                        if ("DatasourceList".equals(type)) {
                            where.append(" AND ? = ANY(\"").append(requireIdent(col)).append("\")");
                        } else {
                            // typed scalar (or a node's display name): compare as text
                            where.append(" AND \"").append(physicalColumn(build, col)).append("\"::text = ?");
                        }
                        binds.add(v);
                    }
                }
            }

            long totalCount;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT count(*) FROM " + tbl + " WHERE " + where)) {
                bind(ps, binds);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    totalCount = rs.getLong(1);
                }
            }

            List<Map<String, Object>> results = new ArrayList<>();
            List<Object> dataBinds = new ArrayList<>(binds);
            dataBinds.add(limit);
            dataBinds.add(offset);
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT convert_from(payload, 'UTF8') FROM " + tbl + " WHERE " + where
                    + " ORDER BY row_number ASC LIMIT ? OFFSET ?")) {
                bind(ps, dataBinds);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        results.add(gson.fromJson(rs.getString(1),
                                new TypeToken<Map<String, Object>>() {}.getType()));
                    }
                }
            }

            Map<String, Map<String, Long>> facets = new LinkedHashMap<>();
            if (facetFields != null && !facetFields.isEmpty() && totalCount < 100_000) {
                for (String facetField : facetFields) {
                    String type = build.columnType(facetField);
                    String valueExpr;
                    String from = tbl;
                    if ("DatasourceList".equals(type)) {
                        valueExpr = "elem";
                        from = tbl + ", LATERAL unnest(\"" + requireIdent(facetField) + "\") AS elem";
                    } else {
                        valueExpr = "\"" + physicalColumn(build, facetField) + "\"::text";
                    }
                    Map<String, Long> counts = new LinkedHashMap<>();
                    try (PreparedStatement ps = conn.prepareStatement(
                            "SELECT " + valueExpr + " AS fv, count(*) AS c FROM " + from
                            + " WHERE " + where + " GROUP BY fv ORDER BY c DESC")) {
                        bind(ps, binds);
                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) {
                                String fv = rs.getString("fv");
                                if (fv != null) counts.put(fv, rs.getLong("c"));
                            }
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

    // ------------------------------------------------------------------
    // Closure-at-query-time serving of materialised parameterised templates
    // ------------------------------------------------------------------
    //
    // A parameterised materialised template stores base-keyed result rows for its
    // whole domain (all cells, all diseases, ...). At query time, given a queried
    // value P, we resolve the set of source CURIEs in P's closure (P itself plus
    // its broad_match descendants/ancestors, or just P for exact) and keep the
    // rows whose base column's `id` array intersects that set. Counts become a
    // cheap count(*) over the filtered rows (flat latency; no live Cypher count).

    /** How to extract a facet value from a stored result column. */
    public enum FacetKind {
        SCALAR,     // data ->> col                       (string columns)
        ARRAY,      // unnest(data -> col)                 (DatasourceList columns)
        NODE_NAME   // data -> col -> 'grebi:name' ->> 0   (GraphNodeId columns)
    }

    /** A column to facet on, plus how to extract its value. */
    public static final class FacetField {
        public final String column;
        public final FacetKind kind;
        public FacetField(String column, FacetKind kind) {
            this.column = column;
            this.kind = kind;
        }
    }

    // Facets are skipped above this many closure-matched rows (too big to GROUP BY
    // interactively), and each facet returns at most this many values.
    private static final long FACET_MAX_ROWS = 200_000;
    private static final int FACET_MAX_VALUES = 50;

    /** One parameter's serving directive: which base column it filters and how. */
    public static class ClosureParam {
        public final String filtersColumn;
        public final String closure;       // descendants | ancestors | exact
        public final String queriedCurie;  // reprefixed source CURIE

        public ClosureParam(String filtersColumn, String closure, String queriedCurie) {
            this.filtersColumn = filtersColumn;
            this.closure = closure;
            this.queriedCurie = queriedCurie;
        }
    }

    /**
     * Resolve the set of source CURIEs in the closure of `queriedCurie`:
     * the queried node itself plus (for descendants/ancestors) the nodes reached
     * via the precomputed `biolink:broad_match` closure. Empty if the queried node
     * is unknown to this graph.
     */
    private Set<String> closureCurieSet(Connection conn, String graph, String closure, String queriedCurie)
            throws SQLException {
        String nodesTbl = "\"nodes_" + graph + "\"";
        String edgesTbl = "\"edges_" + graph + "\"";

        List<String> pnodes = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT \"grebi:nodeId\" FROM " + nodesTbl + " WHERE \"grebi:sourceIds\" && ?")) {
            ps.setArray(1, conn.createArrayOf("text", new String[]{queriedCurie}));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) pnodes.add(rs.getString(1));
            }
        }
        if (pnodes.isEmpty()) {
            return Set.of();
        }

        Set<String> nodeIds = new LinkedHashSet<>(pnodes);
        String c = (closure == null ? "descendants" : closure.toLowerCase());
        if (c.equals("descendants") || c.equals("ancestors")) {
            // broad_match points descendant -> ancestor. Descendants of P are the
            // fromNodeId of broad_match edges into P; ancestors the toNodeId out of
            // P. A single hop suffices — broad_match is a full transitive closure.
            String selectCol = c.equals("descendants") ? "grebi:fromNodeId" : "grebi:toNodeId";
            String matchCol = c.equals("descendants") ? "grebi:toNodeId" : "grebi:fromNodeId";
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT \"" + selectCol + "\" FROM " + edgesTbl +
                    " WHERE \"grebi:type\" = 'biolink:broad_match' AND \"" + matchCol + "\" = ANY(?)")) {
                ps.setArray(1, conn.createArrayOf("text", pnodes.toArray(new String[0])));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) nodeIds.add(rs.getString(1));
                }
            }
        }

        Set<String> curies = new HashSet<>();
        curies.add(queriedCurie);
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT \"grebi:sourceIds\" FROM " + nodesTbl + " WHERE \"grebi:nodeId\" = ANY(?)")) {
            ps.setArray(1, conn.createArrayOf("text", nodeIds.toArray(new String[0])));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    java.sql.Array arr = rs.getArray(1);
                    if (arr != null) {
                        for (Object s : (Object[]) arr.getArray()) {
                            if (s != null) curies.add(s.toString());
                        }
                    }
                }
            }
        }
        return curies;
    }

    /** WHERE clause + ordered bind values for a closure-filtered materialised query. */
    private static final class ClosureWhere {
        final String sql;
        final List<Object> binds;
        final boolean impossible; // a param resolved to an empty closure -> no rows
        ClosureWhere(String sql, List<Object> binds, boolean impossible) {
            this.sql = sql; this.binds = binds; this.impossible = impossible;
        }
    }

    private ClosureWhere buildClosureWhere(Connection conn, String graph,
            List<ClosureParam> params, String searchText) throws SQLException {
        StringBuilder sql = new StringBuilder("TRUE");
        List<Object> binds = new ArrayList<>();

        for (ClosureParam cp : params) {
            Set<String> curies = closureCurieSet(conn, graph, cp.closure, cp.queriedCurie);
            if (curies.isEmpty()) {
                return new ClosureWhere(null, null, true);
            }
            // "<col>_id" TEXT[] && closure — array overlap, satisfied by the
            // column's GIN index (the jsonb_exists_any predicate this replaces
            // could only ever seq-scan).
            sql.append(" AND \"").append(requireIdent(cp.filtersColumn)).append("_id\" && ?");
            binds.add(conn.createArrayOf("text", curies.toArray(new String[0])));
        }

        // Optional free-text narrow over the stored row (coarse, like the /tables
        // browse). Applied on top of the closure filter, so it scans only the
        // already-narrowed subset.
        if (searchText != null && !searchText.isBlank()) {
            sql.append(" AND convert_from(payload, 'UTF8') ILIKE ?");
            binds.add("%" + escapeLike(searchText) + "%");
        }
        return new ClosureWhere(sql.toString(), binds, false);
    }

    private static void bind(PreparedStatement ps, List<Object> binds) throws SQLException {
        for (int i = 0; i < binds.size(); i++) {
            Object b = binds.get(i);
            if (b instanceof java.sql.Array a) ps.setArray(i + 1, a);
            else if (b instanceof Integer n) ps.setInt(i + 1, n);
            else if (b instanceof Long n) ps.setLong(i + 1, n);
            else ps.setString(i + 1, (String) b);
        }
    }

    /**
     * Serve a full-materialise parameterised template from Postgres: filter the
     * stored rows by the closure of each parameter (plus an optional free-text
     * narrow), page, count and — uniquely to the materialised path, since the rows
     * sit in an indexed table — return a top-N value breakdown for each facet
     * column. The live Cypher /query path can't cheaply do the last two.
     */
    public MatQueryResult searchMaterialisedParameterised(
            String graph, MaterialisedBuild build, List<ClosureParam> params,
            String searchText, List<FacetField> facetFields,
            String sortColumn, boolean sortAsc,
            int offset, int limit) {
        if (!graph.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException("Invalid graph name");
        }
        String tbl = "\"" + requireIdent(build.table) + "\"";
        try (Connection conn = getConnection()) {
            ClosureWhere w = buildClosureWhere(conn, graph, params, searchText);
            if (w.impossible) {
                return new MatQueryResult(List.of(), 0, Map.of());
            }

            long totalCount;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT count(*) FROM " + tbl + " WHERE " + w.sql)) {
                bind(ps, w.binds);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    totalCount = rs.getLong(1);
                }
            }

            List<Object> dataBinds = new ArrayList<>(w.binds);
            String orderBy = buildOrderByClause(build, sortColumn, sortAsc);
            dataBinds.add(limit);
            dataBinds.add(offset);

            List<Map<String, Object>> results = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT convert_from(payload, 'UTF8') FROM " + tbl
                    + " WHERE " + w.sql + orderBy + " LIMIT ? OFFSET ?")) {
                bind(ps, dataBinds);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = gson.fromJson(rs.getString(1),
                                new TypeToken<Map<String, Object>>() {}.getType());
                        stripGraphPrefix(row, graph);
                        results.add(row);
                    }
                }
            }

            Map<String, Map<String, Long>> facets =
                    computeFacets(conn, build, w, facetFields, totalCount);

            return new MatQueryResult(results, totalCount, facets);
        } catch (SQLException e) {
            logger.error("Materialised parameterised search failed", e);
            throw new RuntimeException(e);
        }
    }

    /** Top-N value breakdown per facet column over the closure-filtered rows.
     *  Skipped above FACET_MAX_ROWS (too big to GROUP BY interactively). */
    private Map<String, Map<String, Long>> computeFacets(Connection conn, MaterialisedBuild build,
            ClosureWhere w, List<FacetField> facetFields, long totalCount) throws SQLException {
        Map<String, Map<String, Long>> facets = new LinkedHashMap<>();
        if (facetFields == null || facetFields.isEmpty() || totalCount > FACET_MAX_ROWS) {
            return facets;
        }
        String tbl = "\"" + requireIdent(build.table) + "\"";
        for (FacetField f : facetFields) {
            String valueExpr;
            String from = tbl;
            if (f.kind == FacetKind.ARRAY) {
                valueExpr = "elem";
                from = tbl + ", LATERAL unnest(\"" + requireIdent(f.column) + "\") AS elem";
            } else if (f.kind == FacetKind.NODE_NAME) {
                valueExpr = "\"" + requireIdent(f.column) + "_name\"";
            } else {
                valueExpr = "\"" + requireIdent(f.column) + "\"";
            }
            String sql = "SELECT " + valueExpr + " AS fv, count(*) AS c FROM " + from
                    + " WHERE " + w.sql + " GROUP BY fv ORDER BY c DESC LIMIT ?";
            List<Object> binds = new ArrayList<>(w.binds);
            binds.add(FACET_MAX_VALUES);
            Map<String, Long> counts = new LinkedHashMap<>();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                bind(ps, binds);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String fv = rs.getString("fv");
                        if (fv != null) counts.put(fv, rs.getLong("c"));
                    }
                }
            }
            facets.put(f.column, counts);
        }
        return facets;
    }

    /** Build the ORDER BY clause for a closure query. Sorting goes against the
     *  typed column (a GraphNodeId column sorts by its "_name"); a row_number
     *  tiebreaker keeps pagination stable. */
    private String buildOrderByClause(MaterialisedBuild build, String sortColumn, boolean sortAsc) {
        if (sortColumn == null || sortColumn.isBlank()) {
            return " ORDER BY row_number ASC";
        }
        String dir = sortAsc ? "ASC" : "DESC";
        return " ORDER BY \"" + physicalColumn(build, sortColumn) + "\" " + dir
                + " NULLS LAST, row_number ASC";
    }

    /**
     * Stream every closure-matching row of a full-materialise template through a
     * single server-side cursor — the closure is resolved once and no per-page
     * count is run. Used by the CSV export.
     */
    public void streamMaterialisedParameterised(
            String graph, MaterialisedBuild build, List<ClosureParam> params, String searchText,
            String sortColumn, boolean sortAsc,
            java.util.function.Consumer<Map<String, Object>> rowConsumer) {
        if (!graph.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException("Invalid graph name");
        }
        String tbl = "\"" + requireIdent(build.table) + "\"";
        try (Connection conn = getConnection()) {
            boolean prevAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false); // required for a server-side (streaming) cursor
            try {
                ClosureWhere w = buildClosureWhere(conn, graph, params, searchText);
                if (w.impossible) {
                    return;
                }
                List<Object> binds = new ArrayList<>(w.binds);
                String orderBy = buildOrderByClause(build, sortColumn, sortAsc);
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT convert_from(payload, 'UTF8') FROM " + tbl + " WHERE " + w.sql + orderBy)) {
                    ps.setFetchSize(10_000);
                    bind(ps, binds);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            Map<String, Object> row = gson.fromJson(rs.getString(1),
                                    new TypeToken<Map<String, Object>>() {}.getType());
                            stripGraphPrefix(row, graph);
                            rowConsumer.accept(row);
                        }
                    }
                }
                conn.commit();
            } finally {
                conn.setAutoCommit(prevAutoCommit);
            }
        } catch (SQLException e) {
            logger.error("Materialised parameterised stream failed", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Count-only serving: for a counts_only template the stored rows are a compact
     * per-base-node histogram carrying `_count`; the total for a queried value is
     * the sum of `_count` over the base nodes in its closure. Exact, because the
     * materialised rows are DISTINCT and partitioned by base node.
     */
    public long sumMaterialisedParameterisedCounts(
            String graph, MaterialisedBuild build, List<ClosureParam> params) {
        if (!graph.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException("Invalid graph name");
        }
        String tbl = "\"" + requireIdent(build.table) + "\"";
        try (Connection conn = getConnection()) {
            ClosureWhere w = buildClosureWhere(conn, graph, params, null);
            if (w.impossible) {
                return 0;
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT COALESCE(SUM(\"_count\"), 0) FROM " + tbl + " WHERE " + w.sql)) {
                bind(ps, w.binds);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getLong(1);
                }
            }
        } catch (SQLException e) {
            logger.error("Materialised counts sum failed", e);
            throw new RuntimeException(e);
        }
    }

    /** Strip the graph:-prefix Neo4j adds to nodeIds so materialised node columns
     *  match the live (resolve=false) shape. */
    @SuppressWarnings("unchecked")
    private static void stripGraphPrefix(Map<String, Object> row, String graph) {
        if (row == null) return;
        String prefix = graph + ":";
        for (Object v : row.values()) {
            if (v instanceof Map<?, ?> m) {
                Object nid = ((Map<String, Object>) m).get("grebi:nodeId");
                if (nid instanceof String s && s.startsWith(prefix)) {
                    ((Map<String, Object>) m).put("grebi:nodeId", s.substring(prefix.length()));
                }
            }
        }
    }
}
