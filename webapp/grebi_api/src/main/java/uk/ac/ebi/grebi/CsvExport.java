package uk.ac.ebi.grebi;

import com.google.gson.Gson;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;

/**
 * Node and edge lists as CSV, streamed page by page from a paginated search
 * until a short page or the row cap. Lists are joined with "; "; anything
 * structured is written as JSON.
 */
public final class CsvExport {

    /** Rows fetched per page while streaming. */
    public static final int PAGE_SIZE = 1000;
    /** Rows an export stops at, so one request cannot dump a whole graph. */
    public static final int MAX_ROWS = 100_000;

    public static final List<String> NODE_COLUMNS = List.of(
            "grebi:nodeId", "grebi:name", "grebi:type", "grebi:datasources", "grebi:sourceIds", "grebi:curie");
    public static final List<String> EDGE_COLUMNS = List.of(
            "grebi:edgeId", "grebi:type", "grebi:fromNodeId", "from", "grebi:toNodeId", "to", "grebi:datasources", "properties");
    /** Edge row keys that are structure, not properties of the edge. */
    private static final Set<String> EDGE_STRUCTURE = Set.of(
            "grebi:edgeId", "grebi:type", "grebi:fromNodeId", "grebi:toNodeId", "grebi:datasources", "grebi:subgraph",
            "_refs", "from", "to", "id", "_node_ids");

    private static final Gson GSON = new Gson();

    private CsvExport() {}

    /** A page of rows; the export asks for page 0, 1, 2... until a short page. */
    public interface Pages extends IntFunction<List<Map<String, Object>>> {}

    public static void writeNodes(Writer out, Pages pages) throws IOException {
        out.write(row(NODE_COLUMNS));
        stream(pages, node -> {
            var cells = new ArrayList<String>();
            for (var column : NODE_COLUMNS) {
                cells.add(cell(node.get(column)));
            }
            return cells;
        }, out);
    }

    public static void writeEdges(Writer out, Pages pages) throws IOException {
        out.write(row(EDGE_COLUMNS));
        stream(pages, edge -> {
            var properties = new LinkedHashMap<String, Object>();
            for (var entry : edge.entrySet()) {
                if (!EDGE_STRUCTURE.contains(entry.getKey())) {
                    properties.put(entry.getKey(), entry.getValue());
                }
            }
            return List.of(
                    cell(edge.get("grebi:edgeId")),
                    cell(edge.get("grebi:type")),
                    cell(edge.get("grebi:fromNodeId")),
                    nameOf(edge.get("from")),
                    cell(edge.get("grebi:toNodeId")),
                    nameOf(edge.get("to")),
                    cell(edge.get("grebi:datasources")),
                    properties.isEmpty() ? "" : GSON.toJson(properties));
        }, out);
    }

    private static void stream(Pages pages, java.util.function.Function<Map<String, Object>, List<String>> toCells, Writer out) throws IOException {
        int written = 0;
        for (int page = 0; written < MAX_ROWS; page++) {
            var rows = pages.apply(page);
            if (rows == null || rows.isEmpty()) {
                break;
            }
            for (var row : rows) {
                if (written >= MAX_ROWS) {
                    break;
                }
                out.write(row(toCells.apply(row)));
                written++;
            }
            if (rows.size() < PAGE_SIZE) {
                break;
            }
            out.flush();
        }
    }

    /** The first name of a node reference, or its id, or nothing. */
    static String nameOf(Object node) {
        if (!(node instanceof Map<?, ?> ref)) {
            return "";
        }
        var names = ref.get("grebi:name");
        if (names instanceof List<?> list && !list.isEmpty()) {
            return cell(list.get(0));
        }
        if (names instanceof String s) {
            return s;
        }
        return cell(ref.get("grebi:nodeId"));
    }

    /** A value as one cell: lists joined with "; ", maps as JSON, numbers without a trailing .0. */
    static String cell(Object value) {
        if (value == null) {
            return "";
        }
        if (value instanceof String s) {
            return s;
        }
        if (value instanceof List<?> list) {
            var parts = new ArrayList<String>();
            for (var item : list) {
                parts.add(cell(item));
            }
            return String.join("; ", parts);
        }
        if (value instanceof Map<?, ?> map) {
            // a merged value carries its provenance; the bare value is what a table wants
            if (map.containsKey("grebi:value") && map.containsKey("grebi:datasources")) {
                return cell(map.get("grebi:value"));
            }
            if (map.containsKey("grebi:value") && map.containsKey("grebi:properties")) {
                return cell(map.get("grebi:value"));
            }
            return GSON.toJson(map);
        }
        if (value instanceof Double d && d == Math.rint(d) && !Double.isInfinite(d)) {
            return String.valueOf(d.longValue());
        }
        return String.valueOf(value);
    }

    /** One CSV line, quoting a cell when it holds a comma, a quote or a line break. */
    static String row(List<String> cells) {
        var sb = new StringBuilder();
        for (int i = 0; i < cells.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            var cell = cells.get(i);
            if (cell.indexOf(',') >= 0 || cell.indexOf('"') >= 0 || cell.indexOf('\n') >= 0 || cell.indexOf('\r') >= 0) {
                sb.append('"').append(cell.replace("\"", "\"\"")).append('"');
            } else {
                sb.append(cell);
            }
        }
        return sb.append('\n').toString();
    }

    /** A file name from an id: anything unusual becomes an underscore. */
    public static String fileName(String base, String suffix) {
        return base.replaceAll("[^A-Za-z0-9._-]+", "_") + suffix;
    }
}
