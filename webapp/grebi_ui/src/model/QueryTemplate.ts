
export interface QueryTemplate {
  id: string;
  title: string;
  description: string;
  subgraphs: string[];
  topics: string[];
  cypher_match_fragment: string;
  cypher_return_fragment: string;
  cypher_count_fragment: string;
  parameters: Parameter[];
  result_columns: ResultColumn[];
}

export interface Parameter {
  param_id: string;
  param_name: string;
  param_type: string;
  param_opts: Record<string, string>;
}

export interface ResultColumn {
  column_id: string;
  column_type: string;
}

