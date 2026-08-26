package uk.ac.ebi.grebi.db;

import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

/**
 * A materialised query build as recorded in graph_metadata at dataload
 * (materialised_templates for parameterised templates, materialised_queries
 * for standalones). Serving reads its directives — storage table, typed
 * columns, per-param closure filters — from here rather than from the query
 * template YAML, so it always matches the stored rows even when the deployed
 * templates have moved on since the dataload.
 */
public class MaterialisedBuild {

    private static final Gson GSON = new Gson();

    public String id;
    public String subgraph;
    public String mode;    // full | counts_only
    public String table;   // matq_{sg}_{query}; computed once at materialise time
    public List<Column> columns;
    public List<Param> params;

    public static class Column {
        public String column_id;
        public String column_type;
        public Boolean optional;
        public Boolean facet;
    }

    public static class Param {
        public String param_id;
        public String filters_column;
        public String closure;     // descendants | exact
        public String param_type;
    }

    /**
     * Parse a graph_metadata entry, or null if it does not describe a typed
     * table build (pre-typed-table metadata has no `table`; treat as not built
     * so serving falls back to live Cypher).
     */
    public static MaterialisedBuild fromMetadata(JsonElement entry) {
        if (entry == null || !entry.isJsonObject()) {
            return null;
        }
        MaterialisedBuild b = GSON.fromJson(entry, MaterialisedBuild.class);
        if (b.table == null || b.table.isBlank() || b.columns == null || b.columns.isEmpty()) {
            return null;
        }
        // The table name is interpolated into SQL (identifiers cannot be bound);
        // it comes from our own dataload but validate it all the same.
        if (!b.table.matches("[A-Za-z0-9_]+")) {
            return null;
        }
        return b;
    }

    public boolean isCountsOnly() {
        return "counts_only".equalsIgnoreCase(mode == null ? "" : mode);
    }

    public Column column(String columnId) {
        if (columns == null) return null;
        for (var c : columns) {
            if (c.column_id != null && c.column_id.equals(columnId)) {
                return c;
            }
        }
        return null;
    }

    public String columnType(String columnId) {
        var c = column(columnId);
        return c == null || c.column_type == null ? "" : c.column_type;
    }
}
