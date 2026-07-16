
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
  // Present when this template is precomputed into Postgres at dataload.
  // Parameterised materialised templates are served transparently from the
  // same /query/{id} endpoint (Postgres-backed instead of live Cypher).
  materialise?: Materialise;
}

export interface Parameter {
  param_id: string;
  param_name: string;
  param_type: string;
  param_default?: string;
  param_opts: Record<string, string>;
}

export interface ResultColumn {
  column_id: string;
  column_type: string;
  optional?: boolean;
}


export interface Example {
    title: string;
    params: Record<string, any>;
}

export interface Materialise {
  cypher?: string;
  mode?: 'full' | 'counts_only';
  budget_rows?: number;
  run_for_subgraphs?: string[];
  uses_datasources?: string[];
  params?: MaterialiseParam[];
}

export interface MaterialiseParam {
  param_id: string;
  filters_column: string;
  closure?: 'descendants' | 'ancestors' | 'exact';
  domain_root?: string;
}
