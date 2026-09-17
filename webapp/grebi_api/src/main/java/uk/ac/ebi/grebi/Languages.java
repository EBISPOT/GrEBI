package uk.ac.ebi.grebi;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;

/**
 * The languages a node has values in, and the choice of one of them.
 *
 * The dataload keeps a literal in a language other than English as a reified
 * value carrying its language,
 * {@code {"grebi:value":"Gène A","grebi:properties":{"grebi:lang":["fr"]}}},
 * while English and untagged literals stay plain strings. So every value of a
 * node is either untagged, the graph's own language, or tagged with another.
 */
public final class Languages {

    /** Listed on a node that has translations: "en" first, then the others. */
    public static final String LANGUAGES = "grebi:languages";
    public static final String DEFAULT = "en";

    static final String LANG = "grebi:lang";
    static final String VALUE = "grebi:value";
    static final String PROPERTIES = "grebi:properties";
    static final String DATASOURCES = "grebi:datasources";

    private Languages() {}

    /**
     * The language a property value is tagged with, or null when it is untagged.
     * The value may be the merged form, which wraps the value with its
     * datasources and source ids, or the bare value.
     */
    static String languageOf(Object value) {
        Object inner = value;
        if (value instanceof Map<?, ?> wrapper && wrapper.containsKey(DATASOURCES) && wrapper.containsKey(VALUE)) {
            inner = wrapper.get(VALUE);
        }
        if (inner instanceof Map<?, ?> reified
                && reified.get(PROPERTIES) instanceof Map<?, ?> properties
                && properties.get(LANG) instanceof List<?> languages
                && !languages.isEmpty()) {
            return String.valueOf(languages.get(0)).trim().toLowerCase(Locale.ROOT);
        }
        return null;
    }

    /** The languages the node has values in, or an empty list when nothing is translated. */
    public static List<String> languagesOf(Map<String, Object> node) {
        var found = new TreeSet<String>();
        for (var entry : node.entrySet()) {
            if (entry.getValue() instanceof List<?> values) {
                for (var value : values) {
                    var language = languageOf(value);
                    if (language != null) {
                        found.add(language);
                    }
                }
            }
        }
        if (found.isEmpty()) {
            return List.of();
        }
        found.remove(DEFAULT);
        var languages = new ArrayList<String>();
        languages.add(DEFAULT);
        languages.addAll(found);
        return languages;
    }

    /**
     * A copy of the node marked with the languages it has, and, when one is asked
     * for, with that language's values first on every property, the untagged
     * values after them as the fallback, and the other languages dropped. A
     * property left with no values is dropped. Without a language the values are
     * returned as they are.
     */
    public static Map<String, Object> localise(Map<String, Object> node, String lang) {
        if (node == null) {
            return null;
        }
        var localised = new LinkedHashMap<>(node);
        var languages = languagesOf(node);
        if (languages.isEmpty()) {
            return localised;
        }
        localised.put(LANGUAGES, languages);
        if (lang == null || lang.isBlank()) {
            return localised;
        }
        var wanted = lang.trim().toLowerCase(Locale.ROOT);
        for (var entry : node.entrySet()) {
            if (!(entry.getValue() instanceof List<?> values)) {
                continue;
            }
            var chosen = new ArrayList<Object>();
            var untagged = new ArrayList<Object>();
            var translated = false;
            for (var value : values) {
                var language = languageOf(value);
                if (language == null) {
                    untagged.add(value);
                } else {
                    translated = true;
                    if (language.equals(wanted)) {
                        chosen.add(value);
                    }
                }
            }
            if (!translated) {
                continue;
            }
            chosen.addAll(untagged);
            if (chosen.isEmpty()) {
                localised.remove(entry.getKey());
            } else {
                localised.put(entry.getKey(), chosen);
            }
        }
        return localised;
    }
}
