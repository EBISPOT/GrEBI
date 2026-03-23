package uk.ac.ebi.grebi.db;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

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

    private static final List<String> EDGE_FACET_FIELDS = List.of("grebi:datasources");

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
     * Get subgraph names from edge table names (edges_{subgraph} -> subgraph).
     */
    public Set<String> getSubgraphs() {
        Set<String> subgraphs = new LinkedHashSet<>();
        for (String table : listEdgeTables()) {
            if (table.startsWith("edges_")) {
                subgraphs.add(table.substring("edges_".length()));
            }
        }
        return subgraphs;
    }

    private Table<?> edgesTable(String subgraph) {
        if (!subgraph.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException("Invalid subgraph name");
        }
        return table(name("edges_" + subgraph));
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
     * Query edges with pagination and filtering.
     */
    public EdgeQueryResult queryEdges(String subgraph, String filterField, String filterValue,
                                       Map<String, List<String>> extraFilters,
                                       String sortField, String sortDir,
                                       int offset, int limit) {
        try {
            var ctx = dsl();
            var tbl = edgesTable(subgraph);
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
    public EdgeQueryResult queryEdgeRefs(String subgraph, String filterField, String filterValue,
                                          Map<String, List<String>> extraFilters,
                                          String sortField, String sortDir,
                                          int offset, int limit) {
        try {
            var ctx = dsl();
            var tbl = edgesTable(subgraph);
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
    public Map<String, Map<String, Integer>> getEdgeCounts(String subgraph, String filterField, String filterValue) {
        try {
            var ctx = dsl();
            var tbl = edgesTable(subgraph);
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
    public Map<String, Object> getEdgeById(String subgraph, String edgeId) {
        try {
            var ctx = dsl();
            var tbl = edgesTable(subgraph);
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
        Map<String, Map<String, Long>> facets = new LinkedHashMap<>();
        if (facetFields == null || facetFields.isEmpty()) return facets;

        for (String facetField : facetFields) {
            if (!ALLOWED_ARRAY_COLUMNS.contains(facetField)) continue;

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

    private Table<?> nodesTable(String subgraph) {
        if (!subgraph.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException("Invalid subgraph name");
        }
        return table(name("nodes_" + subgraph));
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
    public List<VectorSearchResult> searchByVector(String subgraph, String embeddingModel,
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
            var tbl = nodesTable(subgraph);
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
    public float[] getNodeEmbedding(String subgraph, String nodeId, String embeddingModel) {
        String col = checkedEmbeddingColumn(embeddingModel);

        try {
            Connection conn = getConnection();
            var tbl = nodesTable(subgraph);

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
}
