package uk.ac.ebi.grebi;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.HashSet;

/**
 * A full node cut down to what a reader can take in. A hub node is enormous:
 * the human APOE gene carries 19,832 CTD gene-to-disease associations and
 * references 6,247 other nodes, 20 MB of JSON, which no MCP client can hold
 * (#91). Abridged, each property keeps its first values up to a limit and
 * {@value #TRUNCATED} maps every cut property to its full number of values,
 * so the reader knows what it is not seeing; the referenced-node map keeps
 * the nodes the kept values still mention, and the embedding vectors are
 * left out, as a vector cut short is no vector at all. The node's
 * relationships are available in full, a page at a time, from its edges.
 */
public final class Abridgement {

    /** The property mapping each cut property to the number of values it has in all. */
    public static final String TRUNCATED = "grebi:truncated";

    /** The map of the nodes a node's values refer to, keyed by node id. */
    static final String REFS = "_refs";

    /** The properties holding a node's embedding vectors, one per model. */
    static final String EMBEDDING_PREFIX = "embedding:";

    private Abridgement() {
    }

    /**
     * A copy of the node with at most maxValues values per property, without
     * its embedding vectors, with the referenced nodes it still mentions, and
     * with {@value #TRUNCATED} when a property was cut. A node within the
     * limit is copied unchanged but for the vectors.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> abridge(Map<String, Object> node, int maxValues) {
        if (node == null) {
            return null;
        }
        if (maxValues < 1) {
            throw new IllegalArgumentException("maxValues must be at least 1");
        }
        var abridged = new LinkedHashMap<String, Object>();
        var truncated = new TreeMap<String, Integer>();
        for (var entry : node.entrySet()) {
            var key = entry.getKey();
            var value = entry.getValue();
            if (key.equals(REFS) || key.equals(TRUNCATED) || key.startsWith(EMBEDDING_PREFIX)) {
                continue;
            }
            if (value instanceof List<?> values && values.size() > maxValues) {
                abridged.put(key, new ArrayList<>(values.subList(0, maxValues)));
                truncated.put(key, values.size());
            } else {
                abridged.put(key, value);
            }
        }
        if (node.get(REFS) instanceof Map<?, ?> refs) {
            if (truncated.isEmpty()) {
                abridged.put(REFS, refs);
            } else {
                var mentioned = new HashSet<String>();
                collectMentions(abridged, (Map<String, Object>) refs, mentioned);
                var kept = new LinkedHashMap<String, Object>();
                for (var ref : refs.entrySet()) {
                    if (mentioned.contains(String.valueOf(ref.getKey()))) {
                        kept.put(String.valueOf(ref.getKey()), ref.getValue());
                    }
                }
                abridged.put(REFS, kept);
            }
        }
        if (!truncated.isEmpty()) {
            abridged.put(TRUNCATED, truncated);
        }
        return abridged;
    }

    /** The strings among the values, however deeply nested, that are ids in the refs. */
    private static void collectMentions(Object value, Map<String, Object> refs, Set<String> mentioned) {
        if (value instanceof String s) {
            if (refs.containsKey(s)) {
                mentioned.add(s);
            }
        } else if (value instanceof Map<?, ?> map) {
            for (var v : map.values()) {
                collectMentions(v, refs, mentioned);
            }
        } else if (value instanceof List<?> list) {
            for (var v : list) {
                collectMentions(v, refs, mentioned);
            }
        }
    }
}
