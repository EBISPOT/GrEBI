package uk.ac.ebi.grebi;

import java.util.*;
import java.util.stream.Collectors;

public final class GraphOrder {

    private static final List<String> DEFAULT_PREFERRED_ORDER = List.of("ebi_monarch_xspecies");
    private static final String GRAPH_ORDER_ENV = "GREBI_GRAPH_ORDER";

    private GraphOrder() {}

    public static LinkedHashSet<String> orderedSet(Collection<String> graphs) {
        var preferredOrder = getPreferredOrder();
        var priority = new HashMap<String, Integer>();
        for (int i = 0; i < preferredOrder.size(); i++) {
            priority.put(preferredOrder.get(i), i);
        }

        return graphs.stream()
                .sorted(Comparator
                        .comparingInt((String graph) -> priority.getOrDefault(graph, Integer.MAX_VALUE))
                        .thenComparing(Comparator.naturalOrder()))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static List<String> getPreferredOrder() {
        String configuredOrder = System.getenv(GRAPH_ORDER_ENV);
        if (configuredOrder == null || configuredOrder.isBlank()) {
            return DEFAULT_PREFERRED_ORDER;
        }

        return Arrays.stream(configuredOrder.split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .distinct()
                .toList();
    }
}
