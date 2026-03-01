package uk.ac.ebi.grebi.db;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * Low-level PostgreSQL client for GrEBI edge queries.
 * Manages a connection pool to the pre-built PostgreSQL database.
 */
public class GrebiPostgresClient {

    private static final Logger logger = LoggerFactory.getLogger(GrebiPostgresClient.class);
    private final Gson gson = new Gson();

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

        // Explicitly load the PostgreSQL JDBC driver so DriverManager can find it
        // (the META-INF/services mechanism may not work in uber-jars built by maven-assembly-plugin)
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

    /**
     * Query edges with pagination and filtering.
     */
    public EdgeQueryResult queryEdges(String subgraph, String filterField, String filterValue,
                                       String sortField, String sortDir,
                                       int offset, int limit) {
        String tableName = "edges_" + subgraph;
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT \"_json\" FROM \"").append(tableName).append("\"");
        sql.append(" WHERE \"").append(filterField).append("\" = ?");
        if (sortField != null) {
            sql.append(" ORDER BY \"").append(sortField).append("\"");
            sql.append("asc".equalsIgnoreCase(sortDir) ? " ASC" : " DESC");
        }
        sql.append(" LIMIT ? OFFSET ?");

        // Count query
        String countSql = "SELECT COUNT(*) FROM \"" + tableName + "\" WHERE \"" + filterField + "\" = ?";

        try {
            Connection conn = getConnection();
            long totalCount;
            try (PreparedStatement countStmt = conn.prepareStatement(countSql)) {
                countStmt.setString(1, filterValue);
                try (ResultSet rs = countStmt.executeQuery()) {
                    rs.next();
                    totalCount = rs.getLong(1);
                }
            }

            List<Map<String, Object>> results = new ArrayList<>();
            try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
                stmt.setString(1, filterValue);
                stmt.setInt(2, limit);
                stmt.setInt(3, offset);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        String json = rs.getString("_json");
                        Map<String, Object> map = gson.fromJson(json,
                                new TypeToken<Map<String, Object>>() {}.getType());
                        results.add(map);
                    }
                }
            }

            return new EdgeQueryResult(results, totalCount);
        } catch (SQLException e) {
            logger.error("Edge query failed: {}", sql, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Query edge refs (lightweight: only type, datasources, fromNodeId, toNodeId).
     */
    public EdgeQueryResult queryEdgeRefs(String subgraph, String filterField, String filterValue,
                                          String sortField, String sortDir,
                                          int offset, int limit) {
        String tableName = "edges_" + subgraph;
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT \"grebi:type\", \"grebi:datasources\", \"grebi:fromNodeId\", \"grebi:toNodeId\"");
        sql.append(" FROM \"").append(tableName).append("\"");
        sql.append(" WHERE \"").append(filterField).append("\" = ?");
        if (sortField != null) {
            sql.append(" ORDER BY \"").append(sortField).append("\"");
            sql.append("asc".equalsIgnoreCase(sortDir) ? " ASC" : " DESC");
        }
        sql.append(" LIMIT ? OFFSET ?");

        String countSql = "SELECT COUNT(*) FROM \"" + tableName + "\" WHERE \"" + filterField + "\" = ?";

        try {
            Connection conn = getConnection();
            long totalCount;
            try (PreparedStatement countStmt = conn.prepareStatement(countSql)) {
                countStmt.setString(1, filterValue);
                try (ResultSet rs = countStmt.executeQuery()) {
                    rs.next();
                    totalCount = rs.getLong(1);
                }
            }

            List<Map<String, Object>> results = new ArrayList<>();
            try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
                stmt.setString(1, filterValue);
                stmt.setInt(2, limit);
                stmt.setInt(3, offset);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> ref = new LinkedHashMap<>();
                        ref.put("grebi:type", rs.getString("grebi:type"));
                        Array dsArr = rs.getArray("grebi:datasources");
                        ref.put("grebi:datasources", dsArr != null ? Arrays.asList((String[]) dsArr.getArray()) : List.of());
                        ref.put("grebi:fromNodeId", rs.getString("grebi:fromNodeId"));
                        ref.put("grebi:toNodeId", rs.getString("grebi:toNodeId"));
                        results.add(ref);
                    }
                }
            }

            return new EdgeQueryResult(results, totalCount);
        } catch (SQLException e) {
            logger.error("Edge ref query failed: {}", sql, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Get edge counts grouped by type and datasource.
     */
    public Map<String, Map<String, Integer>> getEdgeCounts(String subgraph, String filterField, String filterValue) {
        String tableName = "edges_" + subgraph;
        String sql = "SELECT \"grebi:type\", ds, COUNT(*) as cnt" +
                " FROM \"" + tableName + "\", UNNEST(\"grebi:datasources\") AS ds" +
                " WHERE \"" + filterField + "\" = ?" +
                " GROUP BY \"grebi:type\", ds";

        Map<String, Map<String, Integer>> result = new LinkedHashMap<>();
        try {
            Connection conn = getConnection();
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, filterValue);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        String type = rs.getString("grebi:type");
                        String datasource = rs.getString("ds");
                        int count = rs.getInt("cnt");
                        result.computeIfAbsent(type, k -> new LinkedHashMap<>()).put(datasource, count);
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("Edge count query failed", e);
            throw new RuntimeException(e);
        }
        return result;
    }

    /**
     * Get a single edge by its ID.
     */
    public Map<String, Object> getEdgeById(String subgraph, String edgeId) {
        String tableName = "edges_" + subgraph;
        String sql = "SELECT \"_json\" FROM \"" + tableName + "\" WHERE \"grebi:edgeId\" = ?";

        try {
            Connection conn = getConnection();
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, edgeId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        return gson.fromJson(rs.getString("_json"),
                                new TypeToken<Map<String, Object>>() {}.getType());
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("Get edge by ID failed", e);
            throw new RuntimeException(e);
        }
        return null;
    }

    public static class EdgeQueryResult {
        public final List<Map<String, Object>> results;
        public final long totalCount;

        public EdgeQueryResult(List<Map<String, Object>> results, long totalCount) {
            this.results = results;
            this.totalCount = totalCount;
        }
    }
}
