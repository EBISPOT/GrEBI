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

    /**
     * The node filter that narrows a search to a closure: the node a CURIE
     * names and its `biolink:broad_match` descendants, which is the value space
     * a query template declares with values_under (so the autosuggest of a
     * disease parameter sends `biolink:broad_match=mondo:0000001`). Repeated,
     * the closures are united.
     */
    public static final String BROAD_MATCH_FILTER = "biolink:broad_match";

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

    /**
     * The array column contains any of the values. Repeated values of one
     * filter are alternatives, as the facets of a search are; a single value
     * is the plain containment the GIN index serves.
     */
    private static Condition arrayHasAnyOf(Field<?> column, List<String> values) {
        if (values.size() == 1) {
            return condition("{0} @> ARRAY[{1}]::text[]", column, inline(values.get(0)));
        }
        var placeholders = new StringBuilder();
        var args = new ArrayList<Object>();
        args.add(column);
        for (int i = 0; i < values.size(); i++) {
            placeholders.append(i == 0 ? "" : ",").append("{").append(i + 1).append("}");
            args.add(inline(values.get(i)));
        }
        return condition("{0} && ARRAY[" + placeholders + "]::text[]", args.toArray());
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
                    conditions.add(arrayHasAnyOf(field(name(key)), values));
                } else if (ALLOWED_COLUMNS.contains(key)) {
                    conditions.add(values.size() == 1 ? checkedColumn(key).eq(values.get(0)) : checkedColumn(key).in(values));
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
            long totalCount = estimateRowCount(ctx, tbl, conditions, "edges_" + graph, unfiltered);
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
                                   String tableName, boolean unfiltered) {
        if (unfiltered) {
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

    /** One direction and edge type of a node, as the graph view asks for them. */
    public static class DirectionAndEdgeType {
        public String direction;
        public String edgeType;
    }

    /**
     * The node at the other end of one edge of each (direction, edge type) of
     * the node, keyed "direction::edgeType". The graph view asks this for the
     * pairs it counted a single edge of, to show that node in place of a count.
     * A pair with no such edge, or whose other end is not a node, is left out.
     */
    public Map<String, Map<String, Object>> resolveSingleEdges(String graph, String nodeId,
                                                                List<DirectionAndEdgeType> items) {
        Map<String, Map<String, Object>> result = new LinkedHashMap<>();
        if (items == null || items.isEmpty()) return result;
        try {
            var ctx = dsl();
            var edges = edgesTable(graph).as("e");
            var nodes = nodesTable(graph).as("n");
            var edgeType = field(name("e", "grebi:type"), String.class);
            var otherId = field(name("n", "grebi:nodeId"), String.class);
            var otherName = field(name("n", "grebi:name"), String.class);

            for (String direction : List.of("incoming", "outgoing")) {
                var types = items.stream()
                        .filter(i -> direction.equals(i.direction) && i.edgeType != null)
                        .map(i -> i.edgeType)
                        .distinct()
                        .toList();
                if (types.isEmpty()) continue;

                boolean incoming = direction.equals("incoming");
                var thisEnd = field(name("e", incoming ? "grebi:toNodeId" : "grebi:fromNodeId"), String.class);
                var otherEnd = field(name("e", incoming ? "grebi:fromNodeId" : "grebi:toNodeId"), String.class);

                for (var record : ctx.select(edgeType, otherId, otherName)
                        .distinctOn(edgeType)
                        .from(edges)
                        .join(nodes).on(otherId.eq(otherEnd))
                        .where(thisEnd.eq(nodeId))
                        .and(edgeType.in(types))
                        .orderBy(edgeType)
                        .fetch()) {
                    Map<String, Object> node = new LinkedHashMap<>();
                    node.put("grebi:nodeId", record.get(otherId));
                    if (record.get(otherName) != null) {
                        node.put("grebi:name", record.get(otherName));
                    }
                    result.put(direction + "::" + record.get(edgeType), node);
                }
            }
            return result;
        } catch (SQLException e) {
            logger.error("Resolving single edges failed", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * The number of nodes in the graph, counted: about a second for the
     * largest graph. (Edges are not counted this way: there are over a billion,
     * and the graph metadata already holds their counts.)
     */
    public long countNodes(String graph) {
        try {
            return dsl().select(field("count(*)", Long.class)).from(nodesTable(graph)).fetchSingle().value1();
        } catch (SQLException e) {
            logger.error("Node count failed", e);
            throw new RuntimeException(e);
        }
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
        return searchByVector(graph, embeddingModel, queryVector, limit, Map.of());
    }

    /**
     * The nodes nearest the vector, among those matching the filters: the
     * array columns (grebi:type, grebi:datasources, grebi:sourceIds) as
     * alternatives when a filter repeats, "-" exclusions, and
     * {@link #BROAD_MATCH_FILTER} closures, as in the node search. Other
     * filter keys are ignored.
     */
    public List<VectorSearchResult> searchByVector(String graph, String embeddingModel,
                                                    float[] queryVector, int limit,
                                                    Map<String, List<String>> filters) {
        String col = checkedEmbeddingColumn(embeddingModel);

        var where = new StringBuilder();
        var arrayParams = new ArrayList<String[]>();
        for (var entry : filters.entrySet()) {
            var values = entry.getValue();
            if (values == null || values.isEmpty()) continue;
            if (entry.getKey().startsWith("-")) {
                var column = entry.getKey().substring(1);
                if (!ALLOWED_NODE_ARRAY_COLUMNS.contains(column)) continue;
                for (var value : values) {
                    where.append(" AND NOT (\"").append(column).append("\" @> ?::text[])");
                    arrayParams.add(new String[] {value});
                }
            } else if (ALLOWED_NODE_ARRAY_COLUMNS.contains(entry.getKey())) {
                where.append(" AND \"").append(entry.getKey()).append("\" && ?::text[]");
                arrayParams.add(values.toArray(new String[0]));
            }
        }

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

            List<String> closureRoots = broadMatchRoots(conn, graph, filters);
            if (closureRoots != null) {
                if (closureRoots.isEmpty()) {
                    return List.of();
                }
                // the closure as in broadMatchCondition, with the roots bound twice
                where.append(" AND \"grebi:nodeId\" IN (SELECT \"grebi:fromNodeId\" FROM ").append(dsl().render(edgesTable(graph)))
                        .append(" WHERE \"grebi:type\" = 'biolink:broad_match' AND \"grebi:toNodeId\" = ANY(?::text[])")
                        .append(" UNION SELECT unnest(?::text[]))");
                var roots = closureRoots.toArray(new String[0]);
                arrayParams.add(roots);
                arrayParams.add(roots);
            }

            // Use raw SQL for the cosine distance operator since jOOQ doesn't natively support <=>
            String sql = "SELECT \"grebi:nodeId\", \"grebi:name\", " +
                    "\"grebi:datasources\", \"grebi:type\", \"grebi:sourceIds\", " +
                    "\"" + col + "\" <=> ?::vector AS distance " +
                    "FROM " + dsl().render(tbl) + " " +
                    "WHERE \"" + col + "\" IS NOT NULL" + where + " " +
                    "ORDER BY distance " +
                    "LIMIT ?";

            List<VectorSearchResult> results = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int p = 1;
                ps.setString(p++, vecLiteral.toString());
                for (var values : arrayParams) {
                    ps.setArray(p++, conn.createArrayOf("text", values));
                }
                ps.setInt(p, limit);
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
     * The stored blob of one node or edge, its JSON deflated, or null when
     * there is none: for a reader that takes the blob as a stream, such as
     * Abridgement.read, and so need not hold a hub node whole.
     */
    public byte[] fetchBlob(String graph, String id) {
        if (!graph.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException("Invalid graph name");
        }
        String sql = "SELECT json FROM \"blobs_" + graph + "\" WHERE id = ?";
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setBytes(1, id.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getBytes(1) : null;
            }
        } catch (Exception e) {
            logger.error("Blob fetch failed for graph {}", graph, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * The stored blobs of several nodes, by id; an id with no blob is not in
     * the map. As {@link #fetchBlob}, in one query.
     */
    public Map<String, byte[]> fetchBlobs(String graph, Collection<String> ids) {
        if (ids == null || ids.isEmpty()) return Map.of();
        if (!graph.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException("Invalid graph name");
        }
        Map<String, byte[]> blobs = new LinkedHashMap<>();
        String sql = "SELECT id, json FROM \"blobs_" + graph + "\" WHERE id = ANY(?)";
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            byte[][] byteIds = ids.stream().map(id -> id.getBytes(java.nio.charset.StandardCharsets.UTF_8)).toArray(byte[][]::new);
            ps.setArray(1, conn.createArrayOf("bytea", byteIds));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    blobs.put(new String(rs.getBytes(1), java.nio.charset.StandardCharsets.UTF_8), rs.getBytes(2));
                }
            }
        } catch (Exception e) {
            logger.error("Blob fetch failed for graph {}", graph, e);
            throw new RuntimeException(e);
        }
        return blobs;
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

    // --- Node references, and the identifiers that name nodes ---

    private static final Field<String> NODE_ID = field(name("grebi:nodeId"), String.class);
    private static final Field<String> NODE_NAME = field(name("grebi:name"), String.class);
    private static final Field<Object> NODE_TYPES = field(name("grebi:type"));
    private static final Field<Object> NODE_DATASOURCES = field(name("grebi:datasources"));
    private static final Field<Object> NODE_SOURCE_IDS = field(name("grebi:sourceIds"));
    private static final Field<String> NODE_CURIE = field(name("grebi:curie"), String.class);
    private static final Field<String> NODE_OLS_CURIE = field(name("ols:curie"), String.class);

    /**
     * The columns a node reference is made of, as the listings, the search and
     * the lookup return it. ols:curie rides along so that a filter on it can
     * be applied to a set of references too; it is not part of the reference.
     */
    private static final Field<?>[] NODE_REF_FIELDS = {
            NODE_ID, NODE_NAME, NODE_TYPES, NODE_DATASOURCES, NODE_SOURCE_IDS, NODE_CURIE, NODE_OLS_CURIE
    };

    private static Map<String, Object> nodeRef(org.jooq.Record record) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("grebi:nodeId", record.get(NODE_ID));
        row.put("grebi:name", record.get(NODE_NAME));
        row.put("grebi:type", toDatasourceList(record.get(NODE_TYPES)));
        row.put("grebi:datasources", toDatasourceList(record.get(NODE_DATASOURCES)));
        row.put("grebi:sourceIds", toDatasourceList(record.get(NODE_SOURCE_IDS)));
        row.put("grebi:curie", record.get(NODE_CURIE));
        return row;
    }

    /** The values as one bound text array, for = ANY and for unnest. */
    private static Field<String[]> textArray(Collection<String> values) {
        return val(values.toArray(new String[0]));
    }

    /**
     * The nodes whose source ids include any of the identifiers, by one GIN
     * probe per identifier. One overlap (&&) with the whole array would ask
     * the same, but the column has no element statistics, so the planner
     * prices every value at 0.5% of the table and, past a few dozen values,
     * leaves the index for a sequential scan. Probed one at a time, each value
     * is an index lookup however many there are. Distinct, since one node may
     * carry several of the identifiers.
     */
    private static Select<org.jooq.Record> nodesBySourceIds(Table<?> tbl, Field<String[]> ids) {
        var probe = unnest(ids).as("probe", "id");
        return selectDistinct(NODE_REF_FIELDS)
                .from(probe)
                .join(tbl).on(condition("{0} @> ARRAY[{1}]::text[]", NODE_SOURCE_IDS, probe.field("id")));
    }

    /** The nodes the condition picks; meant for one a btree index answers. */
    private static Select<org.jooq.Record> nodesWhere(Table<?> tbl, Condition condition) {
        return selectDistinct(NODE_REF_FIELDS).from(tbl).where(condition);
    }

    // --- Bulk identifier lookup ---

    /**
     * The nodes any of the identifiers name, by source id, node id or curie,
     * and by name when lower-cased names are given too. The identifiers are
     * matched exactly as given, so the caller passes every form it wants
     * found. At most limit rows, in node id order, so that the cut-off is
     * deterministic.
     *
     * Each kind of match is its own index-driven query, and UNION joins them.
     * ORed into one WHERE under the ORDER BY and LIMIT, they made the planner
     * walk the node id index in order and filter every row: the whole table
     * for every lookup (#87). The name match has no index to use and scans
     * the table, which is why it is opt-in.
     */
    public List<Map<String, Object>> lookupNodes(String graph, Collection<String> identifiers,
                                                 Collection<String> lowerNames, int limit) {
        var tbl = nodesTable(graph);
        boolean byId = identifiers != null && !identifiers.isEmpty();
        boolean byName = lowerNames != null && !lowerNames.isEmpty();
        if (!byId && !byName) {
            return List.of();
        }
        Select<org.jooq.Record> matched = null;
        if (byId) {
            var ids = textArray(identifiers);
            matched = nodesBySourceIds(tbl, ids)
                    .union(nodesWhere(tbl, NODE_ID.eq(any(ids))))
                    .union(nodesWhere(tbl, NODE_CURIE.eq(any(ids))));
        }
        if (byName) {
            var named = nodesWhere(tbl, lower(NODE_NAME).eq(any(textArray(lowerNames))));
            matched = matched == null ? named : matched.union(named);
        }
        var matches = matched.asTable("matches");
        try {
            List<Map<String, Object>> rows = new ArrayList<>();
            for (var record : dsl().select(matches.fields()).from(matches)
                    .orderBy(matches.field(NODE_ID)).limit(limit).fetch()) {
                rows.add(nodeRef(record));
            }
            return rows;
        } catch (SQLException e) {
            logger.error("Identifier lookup failed for graph {}", graph, e);
            throw new RuntimeException(e);
        }
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
                    conditions.add(arrayHasAnyOf(field(name(key)), values));
                } else if (ALLOWED_NODE_COLUMNS.contains(key)) {
                    var column = field(name(key), String.class);
                    conditions.add(values.size() == 1 ? column.eq(values.get(0)) : column.in(values));
                }
            }
        }
        return conditions;
    }

    /**
     * The roots of a {@link #BROAD_MATCH_FILTER}: the ids of the nodes its
     * CURIEs name. Empty when none names a node of the graph, so the search
     * matches nothing; null when the filters have no such key.
     */
    private List<String> broadMatchRoots(Connection conn, String graph,
                                         Map<String, List<String>> filters) throws SQLException {
        if (filters == null) {
            return null;
        }
        var curies = filters.get(BROAD_MATCH_FILTER);
        if (curies == null || curies.isEmpty()) {
            return null;
        }
        var wanted = curies.stream()
                .filter(c -> c != null && !c.isBlank())
                .map(String::trim)
                .toArray(String[]::new);
        List<String> roots = new ArrayList<>();
        if (wanted.length == 0) {
            return roots;
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT \"grebi:nodeId\" FROM " + dsl().render(nodesTable(graph)) + " WHERE \"grebi:sourceIds\" && ?")) {
            ps.setArray(1, conn.createArrayOf("text", wanted));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) roots.add(rs.getString(1));
            }
        }
        return roots;
    }

    /**
     * The closure of the roots as a condition on the node id: the roots and
     * their broad_match descendants (a single hop: broad_match is a full
     * transitive closure). The closure is computed inside the query from the
     * handful of root ids, so an ontology root's ~40k descendants never leave
     * Postgres; with a name search the planner hash-joins them to the trigram
     * hits (~25ms), and without one it walks the partial broad_match index
     * (~0.2s), never the name index.
     */
    private Condition broadMatchCondition(String graph, List<String> roots) {
        var rootsArray = val(roots.toArray(new String[0]));
        return condition(
                "{0} IN (SELECT {1} FROM {2} WHERE {3} = 'biolink:broad_match' AND {4} = ANY({5}) UNION SELECT unnest({5}))",
                field(name("grebi:nodeId")), GREBI_FROM_NODE_ID, edgesTable(graph), GREBI_TYPE, GREBI_TO_NODE_ID, rootsArray);
    }

    /** The node filters that name nodes outright, the one with the tightest index first. */
    private static final List<String> NODE_IDENTIFIER_FILTERS = List.of(
            "grebi:nodeId", "grebi:curie", "ols:curie", "grebi:sourceIds");

    /**
     * The filter a search starts from: the first identifier filter with a
     * value, or null when there is none. An identifier filter names its nodes
     * outright, so a search with one begins from those few nodes and applies
     * everything else to them.
     */
    private static String seedFilter(Map<String, List<String>> filters) {
        if (filters == null) {
            return null;
        }
        for (var key : NODE_IDENTIFIER_FILTERS) {
            var values = filters.get(key);
            if (values != null && !values.isEmpty()) {
                return key;
            }
        }
        return null;
    }

    /** Under this many matches the facets are computed, and the total is counted rather than estimated. */
    private static final long CHEAP_NODE_FACETS_BELOW = 100_000;

    /**
     * Nodes matching the text and filters. The text matches the name by
     * containment, best similarity first, or, with exactMatch, the whole name
     * ignoring case, in name order. The filters are the array columns
     * (grebi:type, grebi:datasources, grebi:sourceIds) as alternatives when a
     * filter repeats, "-" exclusions, the identifier columns, and
     * {@link #BROAD_MATCH_FILTER}, which keeps only the node a CURIE names and
     * its broad_match descendants.
     *
     * The total is a count whenever the matches are few enough to read: a
     * search seeded by an identifier filter reads them all, and under 100,000
     * expected matches the facets read them anyway, so the page query reads
     * and counts them too. Above that, and for the unfiltered listing, the
     * total is the planner's estimate, which the identifier and name columns
     * have no statistics to inform (#88, #89).
     */
    public NodeQueryResult searchNodes(String graph, String q,
                                        Map<String, List<String>> filters,
                                        boolean exactMatch,
                                        int offset, int limit) {
        try {
            var ctx = dsl();
            var tbl = nodesTable(graph);
            boolean hasText = q != null && !q.isBlank();
            Condition textCondition = null;
            if (hasText) {
                // Both forms are ILIKE, which the trigram index serves; the
                // whole-name match used to be lower(name) = text, which no
                // index does, so it walked the name index and timed out.
                textCondition = exactMatch
                        ? NODE_NAME.likeIgnoreCase(escapeLike(q.trim()))
                        : NODE_NAME.likeIgnoreCase("%" + escapeLike(q) + "%");
            }
            SortField<?> order = hasText && !exactMatch
                    ? field("similarity({0}, {1})", Double.class, NODE_NAME, val(q)).desc()
                    : NODE_NAME.asc();

            Condition closureCondition = null;
            List<String> closureRoots;
            try (Connection conn = getConnection()) {
                closureRoots = broadMatchRoots(conn, graph, filters);
            }
            if (closureRoots != null) {
                if (closureRoots.isEmpty()) {
                    return new NodeQueryResult(List.of(), 0, emptyFacets());
                }
                closureCondition = broadMatchCondition(graph, closureRoots);
            }

            var seedFilter = seedFilter(filters);
            if (seedFilter != null) {
                return searchNodesFromSeed(ctx, tbl, seedFilter, filters, textCondition, closureCondition,
                        order, offset, limit);
            }

            var conditions = buildNodeConditions(filters);
            if (closureCondition != null) {
                conditions.add(closureCondition);
            }
            if (textCondition != null) {
                conditions.add(textCondition);
            }
            boolean unfiltered = conditions.isEmpty();
            // A closure bounds the matches (an ontology root is ~40k node
            // ids, probed by index), so the exact count is cheap, and the
            // planner's estimate of the closure join is far off.
            long total = closureCondition != null
                    ? ctx.select(count()).from(tbl).where(conditions).fetchSingle().value1()
                    : estimateRowCount(ctx, tbl, conditions, "nodes_" + graph, unfiltered);
            boolean counted = !unfiltered && total < CHEAP_NODE_FACETS_BELOW;

            List<Map<String, Object>> results = new ArrayList<>();
            if (counted) {
                // Few enough matches to read: materialise them first, so that
                // their predicates get the index scan they deserve, then count,
                // order and page them from there. Ordered and limited directly,
                // the planner walks the name index filtering row by row until
                // the page is full, which is the whole table when matches are
                // rare; a count(*) window did not stop it, as it priced the
                // window as streaming and limited it early.
                var matched = name("matched").asMaterialized(select(NODE_REF_FIELDS).from(tbl).where(conditions));
                var totalField = field(selectCount().from(matched)).as("_total");
                var fields = new ArrayList<SelectFieldOrAsterisk>(Arrays.asList(NODE_REF_FIELDS));
                fields.add(totalField);
                var page = ctx.with(matched).select(fields).from(matched)
                        .orderBy(order).limit(limit).offset(offset).fetch();
                for (var record : page) {
                    total = record.get(totalField);
                    results.add(nodeRef(record));
                }
                if (page.isEmpty()) {
                    // an empty page carries no count: nothing matched, or the page is past the end
                    total = offset == 0 ? 0 : ctx.with(matched).selectCount().from(matched).fetchSingle().value1();
                }
            } else {
                for (var record : ctx.select(NODE_REF_FIELDS).from(tbl).where(conditions)
                        .orderBy(order).limit(limit).offset(offset).fetch()) {
                    results.add(nodeRef(record));
                }
            }

            Map<String, Map<String, Long>> facets = new LinkedHashMap<>();
            if (!unfiltered && total < CHEAP_NODE_FACETS_BELOW) {
                facets = total == 0 ? emptyFacets()
                        : computeFacets(ctx, tbl, conditions, NODE_FACET_FIELDS, ALLOWED_NODE_ARRAY_COLUMNS);
            }
            return new NodeQueryResult(results, total, facets);
        } catch (SQLException e) {
            logger.error("Node search failed", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * A search seeded by an identifier filter: the nodes its values name, one
     * index probe each, then the other filters and the text applied to those.
     * The set is read whole, a few nodes per identifier at most, so the total,
     * the page and the facets are exact and no plan can stray from the index.
     */
    private NodeQueryResult searchNodesFromSeed(DSLContext ctx, Table<?> tbl, String seedFilter,
                                                Map<String, List<String>> filters, Condition textCondition,
                                                Condition closureCondition, SortField<?> order,
                                                int offset, int limit) {
        var values = textArray(filters.get(seedFilter));
        var seed = name("seed").asMaterialized(seedFilter.equals("grebi:sourceIds")
                ? nodesBySourceIds(tbl, values)
                : nodesWhere(tbl, field(name(seedFilter), String.class).eq(any(values))));
        var rest = new LinkedHashMap<>(filters);
        rest.remove(seedFilter);
        var conditions = buildNodeConditions(rest);
        if (textCondition != null) {
            conditions.add(textCondition);
        }
        if (closureCondition != null) {
            conditions.add(closureCondition);
        }

        List<Map<String, Object>> all = new ArrayList<>();
        for (var record : ctx.with(seed).select(NODE_REF_FIELDS).from(seed)
                .where(conditions).orderBy(order).fetch()) {
            all.add(nodeRef(record));
        }
        var page = offset >= all.size()
                ? List.<Map<String, Object>>of()
                : all.subList(offset, Math.min(all.size(), offset + limit));
        return new NodeQueryResult(new ArrayList<>(page), all.size(), facetsOf(all));
    }

    private static Map<String, Map<String, Long>> emptyFacets() {
        Map<String, Map<String, Long>> facets = new LinkedHashMap<>();
        for (var facetField : NODE_FACET_FIELDS) {
            facets.put(facetField, new LinkedHashMap<>());
        }
        return facets;
    }

    /** The facets of node references in hand: each value's count, the most frequent first, then by name. */
    @SuppressWarnings("unchecked")
    private static Map<String, Map<String, Long>> facetsOf(List<Map<String, Object>> rows) {
        var facets = emptyFacets();
        for (var facetField : NODE_FACET_FIELDS) {
            Map<String, Long> counts = new HashMap<>();
            for (var row : rows) {
                for (var value : (List<String>) row.get(facetField)) {
                    counts.merge(value, 1L, Long::sum);
                }
            }
            var ordered = facets.get(facetField);
            counts.entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed()
                            .thenComparing(Map.Entry.comparingByKey()))
                    .forEach(e -> ordered.put(e.getKey(), e.getValue()));
        }
        return facets;
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

    /**
     * The text a free-text narrow searches: every typed column of the build,
     * joined. Node columns contribute their name, datasource lists their
     * members, numbers their text form. Built with jOOQ so identifiers are
     * quoted and values bound by the query builder.
     *
     * It used to ILIKE over convert_from(payload, 'UTF8'), i.e. decode and scan
     * the stored JSON of every closure-matched row: 12s for the 411k rows of
     * "leukocyte" in gwas_by_cell_type, and the facet queries repeat the WHERE,
     * so the request took 36s and the UI sat on its loading overlay. The typed
     * columns hold the same values and scan in well under a second.
     */
    private static Field<String> searchableText(MaterialisedBuild build) {
        List<Field<?>> parts = new ArrayList<>();
        parts.add(DSL.inline(" "));
        for (MaterialisedBuild.Column c : build.columns) {
            String col = requireIdent(c.column_id);
            String type = c.column_type;
            if ("GraphNodeId".equals(type)) {
                // "<col>_name" exists in every build layout (older builds store
                // the id side as "<col>_id" TEXT[], newer as "<col>_nid").
                parts.add(DSL.field(DSL.name(col + "_name"), String.class));
            } else if ("DatasourceList".equals(type)) {
                parts.add(DSL.function("array_to_string", String.class,
                        DSL.field(DSL.name(col)), DSL.inline(" ")));
            } else if ("float".equals(type) || "int".equals(type)) {
                parts.add(DSL.field(DSL.name(col)).cast(String.class));
            } else {
                parts.add(DSL.field(DSL.name(col), String.class));
            }
        }
        return DSL.function("concat_ws", String.class, parts.toArray(new Field<?>[0]));
    }

    /** The free-text narrow as a jOOQ condition: searchable text ILIKE %text%. */
    private static Condition freeTextCondition(MaterialisedBuild build, String searchText) {
        return searchableText(build).likeIgnoreCase("%" + escapeLike(searchText) + "%");
    }

    /**
     * One facet selection as a jOOQ condition: the row's value in `col` is any
     * of `values`. Several values in one column are alternatives (OR); the
     * selections on different columns AND together in the WHERE. A node column
     * filters on its stored display name (the value its facet breakdown shows),
     * a datasource list on array overlap.
     */
    private static Condition columnFilterCondition(MaterialisedBuild build, String col, List<String> values) {
        String type = build.columnType(col);
        if ("DatasourceList".equals(type)) {
            List<Field<String>> vals = new ArrayList<>();
            for (String v : values) vals.add(DSL.val(v));
            // The bound strings arrive as varchar, and text[] && varchar[] has no
            // operator, so the array is cast (as the node filters above do).
            return DSL.condition("{0} && {1}::text[]", DSL.field(DSL.name(requireIdent(col))), DSL.array(vals));
        }
        Field<String> f = ("float".equals(type) || "int".equals(type))
                ? DSL.field(DSL.name(requireIdent(col))).cast(String.class)
                : DSL.field(DSL.name(physicalColumn(build, col)), String.class);
        return values.size() == 1 ? f.eq(values.get(0)) : f.in(values);
    }

    /** How to facet a stored column, or null if its type has no breakdown. */
    public static FacetField facetField(MaterialisedBuild build, String col) {
        String type = build.columnType(col);
        if ("DatasourceList".equals(type)) return new FacetField(col, FacetKind.ARRAY);
        if ("GraphNodeId".equals(type)) return new FacetField(col, FacetKind.NODE_NAME);
        if ("string".equalsIgnoreCase(type)) return new FacetField(col, FacetKind.SCALAR);
        return null; // float / int / EdgeId
    }

    /**
     * The WHERE of a query over a materialised table: the closure match (TRUE
     * for a standalone table), string-assembled with its array binds, plus jOOQ
     * conditions for the free-text narrow and one per selected facet column.
     * jOOQ renders those with ? placeholders and supplies their bind values, so
     * no predicate text is written by hand. A facet's breakdown is computed with
     * that column's own selection left out, so it keeps listing the values a
     * user could add rather than collapsing to the ones already ticked.
     */
    private static final class MatqWhere {
        // Rendering needs no connection; a dialect-only context does it.
        private static final DSLContext RENDER = DSL.using(SQLDialect.POSTGRES);

        static final MatqWhere IMPOSSIBLE = new MatqWhere(null, null, true);

        private final String closureSql;
        private final List<Object> closureBinds;
        final boolean impossible;   // a param resolved to an empty closure -> no rows
        private Condition text;     // free-text narrow, or null
        private final Map<String, Condition> filters = new LinkedHashMap<>();  // column -> selection

        MatqWhere(String closureSql, List<Object> closureBinds) {
            this(closureSql, closureBinds, false);
        }

        private MatqWhere(String closureSql, List<Object> closureBinds, boolean impossible) {
            this.closureSql = closureSql;
            this.closureBinds = closureBinds == null ? List.of() : closureBinds;
            this.impossible = impossible;
        }

        /** Add the free-text narrow and the facet selections (column -> values). */
        void narrow(MaterialisedBuild build, String searchText, Map<String, List<String>> columnFilters) {
            if (searchText != null && !searchText.isBlank()) {
                text = freeTextCondition(build, searchText);
            }
            if (columnFilters == null) return;
            for (var e : columnFilters.entrySet()) {
                var values = e.getValue();
                if (values == null || values.isEmpty()) continue;
                if (build.column(e.getKey()) == null) {
                    throw new IllegalArgumentException("Unknown filter column: " + e.getKey());
                }
                filters.put(e.getKey(), columnFilterCondition(build, e.getKey(), values));
            }
        }

        String sql() { return sql(null); }
        List<Object> binds() { return binds(null); }

        /** The predicate, leaving out the selection on `excludeColumn` (null: all of it). */
        String sql(String excludeColumn) {
            StringBuilder sb = new StringBuilder(closureSql);
            for (Condition c : conditions(excludeColumn)) {
                sb.append(" AND (").append(RENDER.render(c)).append(")");
            }
            return sb.toString();
        }

        List<Object> binds(String excludeColumn) {
            List<Object> out = new ArrayList<>(closureBinds);
            for (Condition c : conditions(excludeColumn)) {
                out.addAll(RENDER.extractBindValues(c));
            }
            return out;
        }

        private List<Condition> conditions(String excludeColumn) {
            List<Condition> out = new ArrayList<>();
            if (text != null) out.add(text);
            for (var e : filters.entrySet()) {
                if (!e.getKey().equals(excludeColumn)) out.add(e.getValue());
            }
            return out;
        }
    }

    /**
     * The number of rows of a materialised table. Its rows are numbered from 1
     * in row_number, which is indexed, so the highest number is the count
     * without reading the table.
     */
    public long countMaterialisedRows(MaterialisedBuild build) {
        try {
            var highest = dsl().select(max(field(name("row_number"), Integer.class)))
                    .from(table(name(requireIdent(build.table))))
                    .fetchSingle().value1();
            return highest == null ? 0 : highest;
        } catch (SQLException e) {
            logger.error("Counting the rows of {} failed", build.table, e);
            throw new RuntimeException(e);
        }
    }

    /** Browse a standalone materialised query's table (the /tables UI). */
    public MatQueryResult searchMaterialisedQueryResults(
            MaterialisedBuild build, String searchText,
            Map<String, List<String>> filters, List<String> facetFields,
            int offset, int limit) {
        String tbl = "\"" + requireIdent(build.table) + "\"";
        try (Connection conn = getConnection()) {
            MatqWhere w = new MatqWhere("TRUE", List.of());
            w.narrow(build, searchText, filters);

            long totalCount;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT count(*) FROM " + tbl + " WHERE " + w.sql())) {
                bind(ps, w.binds());
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    totalCount = rs.getLong(1);
                }
            }

            List<Map<String, Object>> results = new ArrayList<>();
            List<Object> dataBinds = new ArrayList<>(w.binds());
            dataBinds.add(limit);
            dataBinds.add(offset);
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT convert_from(payload, 'UTF8') FROM " + tbl + " WHERE " + w.sql()
                    + " ORDER BY row_number ASC LIMIT ? OFFSET ?")) {
                bind(ps, dataBinds);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        results.add(gson.fromJson(rs.getString(1),
                                new TypeToken<Map<String, Object>>() {}.getType()));
                    }
                }
            }

            // Facets on the columns the caller asked for; unknown or unfacetable
            // (numeric, edge id) columns are ignored.
            List<FacetField> ff = new ArrayList<>();
            if (facetFields != null) {
                for (String col : facetFields) {
                    FacetField f = build.column(col) == null ? null : facetField(build, col);
                    if (f != null) ff.add(f);
                }
            }
            Map<String, Map<String, Long>> facets = computeFacets(conn, build, w, ff, totalCount);

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
    /**
     * The node ids in the closure of `queriedCurie`: the queried node itself
     * plus (for descendants/ancestors) the nodes reached via the precomputed
     * `biolink:broad_match` closure. Empty if the node is unknown to this graph.
     * Cheap even for an ontology root (~80k nodes in well under a second via the
     * partial broad_match indexes).
     */
    private Set<String> closureNodeIdSet(Connection conn, String graph, String closure, String queriedCurie)
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
        return nodeIds;
    }

    /**
     * The closure of `queriedCurie` as source CURIEs — every clique id of every
     * closure node. Only needed by builds that stored curie arrays; expanding a
     * large closure this way is expensive (an ontology root: ~360k curies, ~50s),
     * which is why newer builds match by node id instead.
     */
    private Set<String> closureCurieSet(Connection conn, String graph, String closure, String queriedCurie)
            throws SQLException {
        String nodesTbl = "\"nodes_" + graph + "\"";
        Set<String> nodeIds = closureNodeIdSet(conn, graph, closure, queriedCurie);
        if (nodeIds.isEmpty()) {
            return Set.of();
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

    private MatqWhere buildClosureWhere(Connection conn, String graph, MaterialisedBuild build,
            List<ClosureParam> params, String searchText, Map<String, List<String>> filters)
            throws SQLException {
        StringBuilder sql = new StringBuilder("TRUE");
        List<Object> binds = new ArrayList<>();

        for (ClosureParam cp : params) {
            if (build.usesNodeIdClosure()) {
                // "<col>_nid" = ANY(closure node ids) — a btree probe per node,
                // no curie expansion, so an ontology-root closure is as cheap to
                // resolve as a leaf's.
                Set<String> nodeIds = closureNodeIdSet(conn, graph, cp.closure, cp.queriedCurie);
                if (nodeIds.isEmpty()) {
                    return MatqWhere.IMPOSSIBLE;
                }
                sql.append(" AND \"").append(requireIdent(cp.filtersColumn)).append("_nid\" = ANY(?)");
                binds.add(conn.createArrayOf("text", nodeIds.toArray(new String[0])));
            } else {
                // Older builds: "<col>_id" TEXT[] && closure curies — array
                // overlap on the column's GIN index.
                Set<String> curies = closureCurieSet(conn, graph, cp.closure, cp.queriedCurie);
                if (curies.isEmpty()) {
                    return MatqWhere.IMPOSSIBLE;
                }
                sql.append(" AND \"").append(requireIdent(cp.filtersColumn)).append("_id\" && ?");
                binds.add(conn.createArrayOf("text", curies.toArray(new String[0])));
            }
        }

        // The free-text narrow (coarse, over the row's typed columns) and the
        // facet selections apply on top of the closure match, so they scan only
        // the already-narrowed subset.
        MatqWhere w = new MatqWhere(sql.toString(), binds);
        w.narrow(build, searchText, filters);
        return w;
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
     * narrow and facet selections), page, count and — uniquely to the materialised path, since the rows
     * sit in an indexed table — return a top-N value breakdown for each facet
     * column. The live Cypher /query path can't cheaply do the last two.
     */
    public MatQueryResult searchMaterialisedParameterised(
            String graph, MaterialisedBuild build, List<ClosureParam> params,
            String searchText, Map<String, List<String>> filters, List<FacetField> facetFields,
            String sortColumn, boolean sortAsc,
            int offset, int limit) {
        if (!graph.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException("Invalid graph name");
        }
        String tbl = "\"" + requireIdent(build.table) + "\"";
        try (Connection conn = getConnection()) {
            MatqWhere w = buildClosureWhere(conn, graph, build, params, searchText, filters);
            if (w.impossible) {
                return new MatQueryResult(List.of(), 0, Map.of());
            }

            // One scan for count + page. The matching (row_number, sort key) set
            // is computed in a MATERIALIZED CTE, which pins the planner to the
            // GIN bitmap scan; the page is then picked from that in memory and
            // only its payloads are fetched, by row_number. Left to itself the
            // planner would sometimes walk the row_number btree filtering each
            // row by the closure predicate — and because materialised rows are
            // clustered by base node, a big closure deep in the ordering meant
            // skipping 145k rows to find the first page (9.5s for leukocyte).
            String sortKey = (sortColumn == null || sortColumn.isBlank())
                    ? "row_number"
                    : "\"" + physicalColumn(build, sortColumn) + "\"";
            String dir = sortAsc ? "ASC" : "DESC";
            String pageOrder = " ORDER BY sk " + dir + " NULLS LAST, row_number ASC";
            String sql = "WITH m AS MATERIALIZED (SELECT row_number, " + sortKey + " AS sk FROM " + tbl
                    + " WHERE " + w.sql() + "),"
                    + " page AS (SELECT row_number, sk FROM m" + pageOrder + " LIMIT ? OFFSET ?)"
                    + " SELECT (SELECT count(*) FROM m) AS total, convert_from(t.payload, 'UTF8') AS payload"
                    + " FROM page JOIN " + tbl + " t ON t.row_number = page.row_number"
                    + " ORDER BY page.sk " + dir + " NULLS LAST, page.row_number ASC";
            List<Object> dataBinds = new ArrayList<>(w.binds());
            dataBinds.add(limit);
            dataBinds.add(offset);

            long totalCount = -1;
            List<Map<String, Object>> results = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                bind(ps, dataBinds);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        totalCount = rs.getLong("total");
                        Map<String, Object> row = gson.fromJson(rs.getString("payload"),
                                new TypeToken<Map<String, Object>>() {}.getType());
                        stripGraphPrefix(row, graph);
                        results.add(row);
                    }
                }
            }
            if (totalCount < 0) {
                // Empty page (no matches, or offset past the end): count separately.
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT count(*) FROM " + tbl + " WHERE " + w.sql())) {
                    bind(ps, w.binds());
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        totalCount = rs.getLong(1);
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

    /** Top-N value breakdown per facet column over the filtered rows, each
     *  computed without that column's own selection (see MatqWhere). Skipped
     *  above FACET_MAX_ROWS (too big to GROUP BY interactively). */
    private Map<String, Map<String, Long>> computeFacets(Connection conn, MaterialisedBuild build,
            MatqWhere w, List<FacetField> facetFields, long totalCount) throws SQLException {
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
                    + " WHERE " + w.sql(f.column) + " GROUP BY fv ORDER BY c DESC LIMIT ?";
            List<Object> binds = new ArrayList<>(w.binds(f.column));
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
            Map<String, List<String>> filters, String sortColumn, boolean sortAsc,
            java.util.function.Consumer<Map<String, Object>> rowConsumer) {
        if (!graph.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException("Invalid graph name");
        }
        String tbl = "\"" + requireIdent(build.table) + "\"";
        try (Connection conn = getConnection()) {
            boolean prevAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false); // required for a server-side (streaming) cursor
            try {
                MatqWhere w = buildClosureWhere(conn, graph, build, params, searchText, filters);
                if (w.impossible) {
                    return;
                }
                List<Object> binds = new ArrayList<>(w.binds());
                String orderBy = buildOrderByClause(build, sortColumn, sortAsc);
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT convert_from(payload, 'UTF8') FROM " + tbl + " WHERE " + w.sql() + orderBy)) {
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
