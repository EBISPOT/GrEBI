
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

    // Optional materialisation capability. When present, this template is
    // precomputed into Postgres at dataload and (for parameterised templates)
    // served from Postgres with closure-at-query-time instead of live Cypher.
    // See docs/materialise-query-templates.md.
    public Materialise materialise;

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

        // full     -> store every result row; serve data + count from Postgres.
        // counts_only -> store a compact per-base-node row count histogram; serve
        //                the count from Postgres (summed over the closure) while
        //                data rows are still served live via Cypher.
        // Default: full.
        public String mode;

        // Optional per-template override of the build-size row budget.
        public Integer budget_rows;

        // Restrict which subgraphs this (standalone) query runs for.
        public List<String> run_for_subgraphs;

        // Display-only list of datasources this query draws on.
        public List<String> uses_datasources;

        public String getMode() {
            return (mode == null || mode.isBlank()) ? "full" : mode;
        }

        public boolean isCountsOnly() {
            return "counts_only".equalsIgnoreCase(getMode());
        }
    }

    public boolean isMaterialised() {
        return materialise != null;
    }

    /** Standalone materialised query (kind 2): a body with no parameters. */
    public boolean isStandaloneMaterialised() {
        return materialise != null && (params == null || params.isEmpty());
    }

    /** Materialised parameterised template (kind 3): derived from fragments. */
    public boolean isParameterisedMaterialised() {
        return materialise != null && params != null && !params.isEmpty();
    }
}
