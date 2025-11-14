
package uk.ac.ebi.grebi.repo;

import java.util.List;
import java.util.Map;

public class QueryTemplate {
    public String id;
    public String title;
    public String description;
    public List<String> subgraphs;
    public List<String> topics;
    public String cypher_match_fragment;
    public String cypher_return_fragment;
    public String cypher_count_fragment;
    public List<Parameter> params;
    public List<ResultColumn> result_columns;
    public List<Example> examples;

    public static class Parameter {
        public String param_id;
        public String param_name;
        public String param_type;
        public String param_default;
        public Map<String, String> param_opts;
    }

    public static class ResultColumn {
        public String column_id;
        public String column_type;
        public Boolean optional;
    }
    public static class Example {
        public String title;
        public Map<String, String> params;
    }

}
