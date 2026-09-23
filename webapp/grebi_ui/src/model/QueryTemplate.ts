
export interface QueryTemplate {
  id: string;
  title: string;
  question: string;
  description: string;
  graphs: string[];
  topics: string[];
  cypher_match_fragment: string;
  cypher_return_fragment: string;
  cypher_count_fragment: string;
  params: Parameter[];
  result_columns: ResultColumn[];
  examples: Example[];
  // Materialisation settings, when the YAML has a `materialise:` block.
  materialise?: Materialise;
  // Whether the template is precomputed into Postgres at dataload — the
  // default; false when it opts out with `materialise: false`. Materialised
  // parameterised templates are served transparently from the same
  // /query/{id} endpoint (Postgres-backed instead of live Cypher).
  materialised?: boolean;
}

export interface Parameter {
  param_id: string;
  param_name: string;
  param_type: string;
  param_default?: string;
  // Value space of a SourceId parameter (mutually exclusive; both absent =
  // unconstrained): this node or a broad_match descendant of it / any node
  // carrying this type label.
  values_under?: string;
  values_with_type?: string;
}

// Search filters narrowing autosuggest to the parameter's declared value space.
export function paramSuggestFilters(param: Parameter): URLSearchParams | undefined {
  if (param.values_under) {
    return new URLSearchParams({ "biolink:broad_match": param.values_under });
  }
  if (param.values_with_type) {
    return new URLSearchParams({ "grebi:type": param.values_with_type });
  }
  return undefined;
}

export interface ResultColumn {
  column_id: string;
  column_type: string;
  optional?: boolean;
  // materialised serving returns a top-N value breakdown for this column
  facet?: boolean;
}


export interface Example {
    title: string;
    params: Record<string, any>;
}

// Settings only: every template is materialised unless its YAML says
// `materialise: false` (see `materialised`).
export interface Materialise {
  cypher?: string;
  budget_rows?: number;
  allow_empty?: boolean;
  run_for_subgraphs?: string[];
  uses_datasources?: string[];
}
