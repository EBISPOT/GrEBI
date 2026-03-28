
export interface QueryTemplate {
  id: string;
  title: string;
  question: string;
  description: string;
  subgraphs: string[];
  topics: string[];
  cypher_match_fragment: string;
  cypher_return_fragment: string;
  cypher_count_fragment: string;
  params: Parameter[];
  result_columns: ResultColumn[];
  examples: Example[];
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
}


export interface Example {
    title: string;
    params: Record<string, any>;
}
