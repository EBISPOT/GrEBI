
package uk.ac.ebi.grebi.repo;

import java.util.List;
import java.util.Map;

public class QueryTemplate {
    public String id;
    public String title;
    public String question;
    public String description;
    public List<String> graphs;
    public List<String> topics;
    public String cypher_match_fragment;
    public String cypher_return_fragment;
    public String cypher_count_fragment;
    public List<Parameter> params;
    public List<ResultColumn> result_columns;
    public List<Example> examples;

    // Materialisation settings. Every template is precomputed into Postgres at
    // dataload (and, if parameterised, served from Postgres with the closure
    // applied at query time) unless its YAML says `materialise: false`. The
    // optional `materialise:` block carries settings only; it is null here when
    // absent or false. See docs/materialise-query-templates.md.
    public Materialise materialise;

    // Whether this template is materialised — false when it opts out, or has
    // nothing to materialise (no params and no materialise.cypher). Derived by
    // the loader; serialised for the UI.
    public boolean materialised;

    private transient boolean materialiseOptOut;

    /**
     * SnakeYAML entry point for `materialise:` (see GrebiQueryTemplatesRepo): the
     * value is `false` (opt out), null/absent, or a settings mapping.
     */
    public void setMaterialiseSetting(Object value) {
        if (value == null) {
            materialise = null;
            materialiseOptOut = false;
        } else if (Boolean.FALSE.equals(value)) {
            materialise = null;
            materialiseOptOut = true;
        } else if (value instanceof Map<?, ?> map) {
            materialise = Materialise.fromMap(map);
            materialiseOptOut = false;
        } else {
            throw new IllegalArgumentException(
                    "materialise must be false or a settings mapping, not " + value);
        }
    }

    public Object getMaterialiseSetting() {
        return materialiseOptOut ? Boolean.FALSE : materialise;
    }

    public static class Parameter {
        public String param_id;
        public String param_name;
        public String param_type;
        public String param_default;
        // Value space of a SourceId parameter (mutually exclusive; both absent
        // = unconstrained). Everything else — which result column it filters,
        // how serving matches stored rows — is derived at dataload (see
        // grebi_materialise.py) and served from graph_metadata (MaterialisedBuild).
        // values_under: value is this node or a broad_match descendant of it.
        public String values_under;
        // values_with_type: value is any node carrying this type label.
        public String values_with_type;
    }

    public static class ResultColumn {
        public String column_id;
        public String column_type;
        public Boolean optional;
        // When true, materialised serving returns a top-N value breakdown for this
        // column (a GROUP BY over the closure-filtered rows). Only meaningful for
        // low-cardinality columns (datasource lists, node names, short strings).
        public Boolean facet;
    }
    public static class Example {
        public String title;
        public Map<String, String> params;
    }

    public static class Materialise {
        // Standalone materialised query body (no params). Degenerate case where
        // the body IS the materialise query (was `cypher_query` in
        // materialised_queries/). Null for parameterised templates, whose
        // materialise query is derived from the fragments at dataload.
        public String cypher;

        // Optional per-template override of the build-size row budget.
        public Integer budget_rows;

        // A template that legitimately materialises no rows for a graph.
        public Boolean allow_empty;

        // Restrict which subgraphs this (standalone) query runs for.
        public List<String> run_for_subgraphs;

        // Display-only list of datasources this query draws on.
        public List<String> uses_datasources;

        private static final java.util.Set<String> KEYS = java.util.Set.of(
                "cypher", "budget_rows", "allow_empty", "run_for_subgraphs", "uses_datasources");

        @SuppressWarnings("unchecked")
        static Materialise fromMap(Map<?, ?> map) {
            for (Object key : map.keySet()) {
                if ("mode".equals(key)) {
                    throw new IllegalArgumentException("materialise.mode is no longer supported: "
                            + "every template is materialised in full unless it sets materialise: false");
                }
                if (!KEYS.contains(key)) {
                    throw new IllegalArgumentException("Unknown materialise setting: " + key);
                }
            }
            var m = new Materialise();
            m.cypher = (String) map.get("cypher");
            m.budget_rows = map.get("budget_rows") == null ? null : ((Number) map.get("budget_rows")).intValue();
            m.allow_empty = (Boolean) map.get("allow_empty");
            m.run_for_subgraphs = (List<String>) map.get("run_for_subgraphs");
            m.uses_datasources = (List<String>) map.get("uses_datasources");
            return m;
        }
    }

    private boolean hasParams() {
        return params != null && !params.isEmpty();
    }

    /** Materialised unless opted out; needs params, or a standalone cypher body. */
    public boolean isMaterialised() {
        return !materialiseOptOut
                && (hasParams() || (materialise != null && materialise.cypher != null));
    }

    /** Standalone materialised query: a cypher body with no parameters. */
    public boolean isStandaloneMaterialised() {
        return isMaterialised() && !hasParams();
    }

    /** Materialised parameterised template: derived from fragments. */
    public boolean isParameterisedMaterialised() {
        return isMaterialised() && hasParams();
    }
}
