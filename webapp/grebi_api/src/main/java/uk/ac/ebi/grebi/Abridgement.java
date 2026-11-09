package uk.ac.ebi.grebi;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.HashSet;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

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

    /** The most values per property a caller may ask for: 500 keep the largest hub nodes under a few megabytes. */
    public static final int MAX_MAX_VALUES = 500;

    private static final Gson GSON = new Gson();

    private Abridgement() {
    }

    /**
     * The node stored in a blob (its JSON, deflated) in the language asked
     * for and abridged: what {@code abridge(Languages.localise(node, lang),
     * maxValues, propertyRefs)} makes of the whole node, without holding the
     * whole node. A hub node's blob inflates to over 100 MB, nearly all of it
     * the values and referenced nodes that an abridgement leaves out, so the
     * blob is read as a stream that keeps each property's first values and
     * skips the rest. Where one pass over the stream cannot tell what the
     * whole node would give (see {@link #readStreamed}), the whole node is
     * read after all. Null for no blob.
     */
    public static Map<String, Object> read(byte[] blob, int maxValues, String lang, boolean propertyRefs) {
        if (blob == null) {
            return null;
        }
        if (maxValues < 1) {
            throw new IllegalArgumentException("maxValues must be at least 1");
        }
        try {
            var streamed = readStreamed(blob, maxValues, lang, propertyRefs);
            if (streamed != null) {
                return streamed;
            }
            try (var reader = inflated(blob)) {
                Map<String, Object> node = GSON.fromJson(reader, new TypeToken<Map<String, Object>>() {}.getType());
                return abridge(Languages.localise(node, lang), maxValues, propertyRefs);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** As {@link #read(byte[], int, String, boolean)} for a page, with the nodes that name the properties. */
    public static Map<String, Object> read(byte[] blob, int maxValues, String lang) {
        return read(blob, maxValues, lang, true);
    }

    /**
     * The nodes of some hits (rows with a grebi:nodeId), each read abridged
     * from its blob in the map, in the hits' order. A hit whose blob is not
     * there is abridged as the row it is.
     */
    public static List<Map<String, Object>> readHits(
        List<Map<String, Object>> hits, Map<String, byte[]> blobs, int maxValues, String lang, boolean propertyRefs
    ) {
        var nodes = new ArrayList<Map<String, Object>>();
        for (var hit : hits) {
            var blob = blobs.get(String.valueOf(hit.get("grebi:nodeId")));
            nodes.add(blob != null ? read(blob, maxValues, lang, propertyRefs) : abridge(hit, maxValues, propertyRefs));
        }
        return nodes;
    }

    private static Reader inflated(byte[] blob) {
        return new InputStreamReader(new InflaterInputStream(new ByteArrayInputStream(blob), new Inflater(), 1 << 16), StandardCharsets.UTF_8);
    }

    /**
     * The abridged node from one pass over the blob, or null when one pass is
     * not enough: when a property follows the referenced nodes, which are
     * chosen by what the properties mention; when a value that is left out (one
     * past a property's first maxValues, or in a property the abridgement
     * drops) carries a language, as a node's languages are those of all its
     * values; and when a language is asked for and a property cut short has
     * values in a language, as choosing a language drops values and brings
     * later ones forward; and for a node that stores a list of its languages,
     * which no node does. The dataload writes the referenced nodes last, and
     * the long properties of a hub node are its relationships, which carry no
     * language, so a hub node is read in one pass.
     */
    static Map<String, Object> readStreamed(byte[] blob, int maxValues, String lang, boolean propertyRefs) throws IOException {
        var languageAsked = lang != null && !lang.isBlank();
        var node = new LinkedHashMap<String, Object>();
        var truncated = new TreeMap<String, Integer>();
        Map<String, Object> localised = null;
        Map<String, Object> refs = null;
        try (var in = new JsonReader(inflated(blob))) {
            // as Gson reads a whole document
            in.setLenient(true);
            if (in.peek() != JsonToken.BEGIN_OBJECT) {
                return null;
            }
            in.beginObject();
            while (in.hasNext()) {
                var key = in.nextName();
                if (key.equals(TRUNCATED) || key.startsWith(EMBEDDING_PREFIX)) {
                    // left out, but a language in it would still be one of the node's
                    if (skipSeeingLanguage(in)) {
                        return null;
                    }
                } else if (key.equals(REFS)) {
                    if (localised != null) {
                        return null;
                    }
                    localised = localised(node, lang, maxValues, truncated);
                    if (in.peek() != JsonToken.BEGIN_OBJECT) {
                        in.skipValue();
                        continue;
                    }
                    // with nothing cut every referenced node stays; else the ones the kept
                    // values mention and, if asked for, the ones that name the properties
                    Set<String> mentioned = null;
                    if (!truncated.isEmpty()) {
                        mentioned = propertyRefs ? new HashSet<>(localised.keySet()) : new HashSet<>();
                        collectStrings(localised, mentioned);
                    }
                    refs = new LinkedHashMap<>();
                    in.beginObject();
                    while (in.hasNext()) {
                        var id = in.nextName();
                        if (mentioned == null || mentioned.contains(id)) {
                            refs.put(id, GSON.fromJson(in, Object.class));
                        } else {
                            in.skipValue();
                        }
                    }
                    in.endObject();
                } else if (localised != null || node.containsKey(key) || key.equals(Languages.LANGUAGES)) {
                    return null;
                } else if (in.peek() == JsonToken.BEGIN_ARRAY) {
                    var kept = new ArrayList<Object>();
                    var size = 0;
                    var inALanguage = false;
                    in.beginArray();
                    while (in.hasNext()) {
                        if (size < maxValues) {
                            Object value = GSON.fromJson(in, Object.class);
                            kept.add(value);
                            inALanguage |= Languages.languageOf(value) != null;
                        } else if (skipSeeingLanguage(in)) {
                            return null;
                        }
                        size++;
                    }
                    in.endArray();
                    if (size > maxValues) {
                        if (languageAsked && inALanguage) {
                            return null;
                        }
                        truncated.put(key, size);
                    }
                    node.put(key, kept);
                } else {
                    node.put(key, GSON.fromJson(in, Object.class));
                }
            }
            in.endObject();
        }
        if (localised == null) {
            localised = localised(node, lang, maxValues, truncated);
        }
        if (refs != null) {
            localised.put(REFS, refs);
        }
        if (!truncated.isEmpty()) {
            localised.put(TRUNCATED, truncated);
        }
        return localised;
    }

    /**
     * The properties read, in the language asked for. The list of the node's
     * languages, which this adds, is a property like any other and is cut to
     * maxValues too.
     */
    private static Map<String, Object> localised(Map<String, Object> node, String lang, int maxValues, Map<String, Integer> truncated) {
        var localised = Languages.localise(node, lang);
        if (localised.get(Languages.LANGUAGES) instanceof List<?> languages && languages.size() > maxValues) {
            localised.put(Languages.LANGUAGES, new ArrayList<>(languages.subList(0, maxValues)));
            truncated.put(Languages.LANGUAGES, languages.size());
        }
        return localised;
    }

    /** Skips the value the reader is at, and says whether a property named grebi:lang is anywhere inside it. */
    private static boolean skipSeeingLanguage(JsonReader in) throws IOException {
        var seen = false;
        switch (in.peek()) {
            case BEGIN_OBJECT -> {
                in.beginObject();
                while (in.hasNext()) {
                    seen |= in.nextName().equals(Languages.LANG);
                    seen |= skipSeeingLanguage(in);
                }
                in.endObject();
            }
            case BEGIN_ARRAY -> {
                in.beginArray();
                while (in.hasNext()) {
                    seen |= skipSeeingLanguage(in);
                }
                in.endArray();
            }
            default -> in.skipValue();
        }
        return seen;
    }

    /** The strings among the values, however deeply nested. */
    private static void collectStrings(Object value, Set<String> strings) {
        if (value instanceof String s) {
            strings.add(s);
        } else if (value instanceof Map<?, ?> map) {
            for (var v : map.values()) {
                collectStrings(v, strings);
            }
        } else if (value instanceof List<?> list) {
            for (var v : list) {
                collectStrings(v, strings);
            }
        }
    }

    /**
     * A copy of the node with at most maxValues values per property, without
     * its embedding vectors, with the referenced nodes it still mentions, and
     * with {@value #TRUNCATED} when a property was cut. A node within the
     * limit is copied unchanged but for the vectors.
     */
    public static Map<String, Object> abridge(Map<String, Object> node, int maxValues) {
        return abridge(node, maxValues, false);
    }

    /**
     * As {@link #abridge(Map, int)}; with propertyRefs the referenced nodes
     * that name the node's own properties are kept too. A page that labels
     * the properties needs them (biolink:affects is "affects"); the MCP tools
     * leave them out, as an agent reads the property names themselves and a
     * hub node has some 20 kB of them.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> abridge(Map<String, Object> node, int maxValues, boolean propertyRefs) {
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
                if (propertyRefs) {
                    mentioned.addAll(abridged.keySet());
                }
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
