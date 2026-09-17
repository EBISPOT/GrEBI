package uk.ac.ebi.grebi;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ebi.grebi.db.PrefixService;
import uk.ac.ebi.grebi.repo.GrebiPostgresRepo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * The bulk identifier lookup: a list of identifiers in, the nodes each one
 * names out. An identifier is matched as given, lower-cased, and normalised to
 * the graph's own CURIE by the prefix service, against every node's source
 * ids, node id and curie; with matchNames, a node's name matches too, ignoring
 * case. The answer keeps the identifiers in the order they were given.
 */
public final class NodeLookup {

    private static final Logger logger = LoggerFactory.getLogger(NodeLookup.class);

    /** How many nodes one identifier may reasonably name; the query stops at this many per identifier. */
    static final int ROWS_PER_ID = 10;

    private final GrebiPostgresRepo postgres;
    private final ResourceLimits limits;
    private final Function<List<String>, List<String>> normaliser;

    /**
     * @param normaliser turns identifiers into the graph's canonical CURIEs, one
     *                   out per one in; it may hand back the input unchanged.
     */
    public NodeLookup(GrebiPostgresRepo postgres, ResourceLimits limits, Function<List<String>, List<String>> normaliser) {
        this.postgres = postgres;
        this.limits = limits;
        this.normaliser = normaliser;
    }

    /**
     * The prefix service as a normaliser. It needs the prefix map in Postgres
     * and the grebi_reprefix binary; when it fails the identifiers are matched
     * as given, which is what a lookup without it would do anyway.
     */
    public static Function<List<String>, List<String>> prefixServiceNormaliser() {
        return ids -> {
            try {
                return PrefixService.get().reprefix(ids);
            } catch (RuntimeException e) {
                logger.warn("Identifiers are matched as given: the prefix service failed", e);
                return ids;
            }
        };
    }

    /**
     * The identifiers of a request body: a JSON object with an "ids" array, a
     * JSON array, or plain text with one identifier per line. Each is trimmed;
     * blanks and repeats are dropped, the order is kept.
     */
    public static List<String> parseIds(String body) {
        var text = body == null ? "" : body.strip();
        List<String> raw = new ArrayList<>();
        try {
            if (text.startsWith("{")) {
                var ids = JsonParser.parseString(text).getAsJsonObject().get("ids");
                if (ids == null || !ids.isJsonArray()) {
                    throw badRequest("The body must have an \"ids\" array");
                }
                for (var element : ids.getAsJsonArray()) {
                    raw.add(idOf(element));
                }
            } else if (text.startsWith("[")) {
                for (var element : JsonParser.parseString(text).getAsJsonArray()) {
                    raw.add(idOf(element));
                }
            } else {
                raw.addAll(List.of(text.split("\\r?\\n")));
            }
        } catch (IllegalStateException | com.google.gson.JsonParseException e) {
            throw badRequest("The body must be a JSON object with an \"ids\" array, a JSON array, or one identifier per line");
        }
        var ids = new LinkedHashSet<String>();
        for (var id : raw) {
            var trimmed = id.strip();
            if (!trimmed.isEmpty()) {
                ids.add(trimmed);
            }
        }
        return new ArrayList<>(ids);
    }

    private static String idOf(JsonElement element) {
        if (!element.isJsonPrimitive()) {
            throw badRequest("Identifiers must be strings");
        }
        return element.getAsString();
    }

    /** The forms an identifier is looked for in. */
    static Set<String> candidates(String id, String normalised) {
        var forms = new LinkedHashSet<String>();
        forms.add(id);
        forms.add(id.toLowerCase(Locale.ROOT));
        if (normalised != null && !normalised.isBlank()) {
            forms.add(normalised);
            forms.add(normalised.toLowerCase(Locale.ROOT));
        }
        return forms;
    }

    /**
     * The lookup as the API answers it: "results", one entry per identifier in
     * the order given with the nodes it names; "notFound", the identifiers with
     * none; and "truncated", true when the query hit its row limit, so that some
     * matches may be missing.
     */
    public Map<String, Object> lookup(String graph, List<String> ids, boolean matchNames, boolean resolve, String lang) {
        if (ids.isEmpty()) {
            throw badRequest("No identifiers given");
        }
        limits.validateLookupIdsCount(ids.size());
        for (var id : ids) {
            limits.validateText(id, "identifier");
        }

        var normalised = normaliser.apply(ids);
        if (normalised == null || normalised.size() != ids.size()) {
            normalised = ids;
        }

        var forms = new LinkedHashSet<String>();
        for (int i = 0; i < ids.size(); i++) {
            forms.addAll(candidates(ids.get(i), normalised.get(i)));
        }
        List<String> lowerNames = matchNames
            ? ids.stream().map(id -> id.toLowerCase(Locale.ROOT)).distinct().toList()
            : null;

        int limit = ids.size() * ROWS_PER_ID;
        var rows = postgres.lookupNodes(graph, forms, lowerNames, limit + 1);
        boolean truncated = rows.size() > limit;
        if (truncated) {
            rows = rows.subList(0, limit);
        }

        var matches = match(ids, normalised, rows, matchNames);

        Map<String, Map<String, Object>> resolved = Map.of();
        if (resolve) {
            var nodeIds = new LinkedHashSet<String>();
            for (var nodes : matches.values()) {
                for (var node : nodes) {
                    nodeIds.add(String.valueOf(node.get("grebi:nodeId")));
                }
            }
            resolved = nodeIds.isEmpty() ? Map.of() : postgres.getPgClient().resolveToMap(graph, nodeIds);
        }

        var results = new ArrayList<Map<String, Object>>();
        var notFound = new ArrayList<String>();
        for (var id : ids) {
            var nodes = new ArrayList<Map<String, Object>>();
            for (var row : matches.get(id)) {
                var blob = resolved.get(String.valueOf(row.get("grebi:nodeId")));
                nodes.add(blob != null ? Languages.localise(blob, lang) : row);
            }
            if (nodes.isEmpty()) {
                notFound.add(id);
            }
            var entry = new LinkedHashMap<String, Object>();
            entry.put("id", id);
            entry.put("nodes", nodes);
            results.add(entry);
        }

        var answer = new LinkedHashMap<String, Object>();
        answer.put("results", results);
        answer.put("notFound", notFound);
        answer.put("truncated", truncated);
        return answer;
    }

    /**
     * Which of the rows each identifier names, in row order. An identifier
     * names a row when one of its forms is among the row's identifiers, exactly
     * as the query matched them, or, with matchNames, equals the row's name
     * ignoring case. Only the forms decide, so whether an identifier matches
     * never depends on which other identifiers were looked up with it.
     */
    static Map<String, List<Map<String, Object>>> match(
        List<String> ids, List<String> normalised, List<Map<String, Object>> rows, boolean matchNames
    ) {
        var matches = new LinkedHashMap<String, List<Map<String, Object>>>();
        for (var id : ids) {
            matches.put(id, new ArrayList<>());
        }
        for (var row : rows) {
            var rowIds = new LinkedHashSet<String>(strings(row.get("grebi:sourceIds")));
            for (var key : List.of("grebi:nodeId", "grebi:curie")) {
                rowIds.addAll(strings(row.get(key)));
            }
            var rowNames = new LinkedHashSet<String>();
            if (matchNames) {
                for (var name : strings(row.get("grebi:name"))) {
                    rowNames.add(name.toLowerCase(Locale.ROOT));
                }
            }
            for (int i = 0; i < ids.size(); i++) {
                var id = ids.get(i);
                boolean named = false;
                for (var form : candidates(id, normalised.get(i))) {
                    if (rowIds.contains(form)) {
                        named = true;
                        break;
                    }
                }
                if (!named && matchNames && rowNames.contains(id.toLowerCase(Locale.ROOT))) {
                    named = true;
                }
                if (named) {
                    matches.get(id).add(row);
                }
            }
        }
        return matches;
    }

    /** A row value as strings: a list of them, a single one, or none. */
    private static List<String> strings(Object value) {
        if (value == null) {
            return List.of();
        }
        if (value instanceof Collection<?> collection) {
            var list = new ArrayList<String>();
            for (var element : collection) {
                if (element instanceof Map<?, ?> wrapped && wrapped.get("grebi:value") != null) {
                    list.add(String.valueOf(wrapped.get("grebi:value")));
                } else if (element != null) {
                    list.add(String.valueOf(element));
                }
            }
            return list;
        }
        return List.of(String.valueOf(value));
    }

    private static ResourceLimits.ResourceLimitException badRequest(String message) {
        return new ResourceLimits.ResourceLimitException(400, message);
    }
}
