import { QueryTemplate } from "../../model/QueryTemplate";

export interface QueryTemplateFilters {
  selectedTopics: Set<string>;
  selectedInputs: Set<string>;
  selectedOutputs: Set<string>;
}

export function getQueryTemplateInputIds(template: QueryTemplate): string[] {
  return (template.params || []).map((param) => param.param_id);
}

export function getQueryTemplateOutputIds(template: QueryTemplate): string[] {
  return (template.result_columns || []).map((column) => column.column_id);
}

export function getAvailableQueryTemplateInputs(queries: QueryTemplate[]): string[] {
  return Array.from(new Set(queries.flatMap(getQueryTemplateInputIds))).sort((a, b) => a.localeCompare(b));
}

export function getAvailableQueryTemplateOutputs(queries: QueryTemplate[]): string[] {
  return Array.from(new Set(queries.flatMap(getQueryTemplateOutputIds))).sort((a, b) => a.localeCompare(b));
}

export function filterQueryTemplates(
  queries: QueryTemplate[],
  filters: QueryTemplateFilters
): QueryTemplate[] {
  return queries.filter((query) => {
    const topicMatches =
      filters.selectedTopics.size === 0 ||
      (query.topics || []).some((topicId) => filters.selectedTopics.has(topicId));

    const inputIds = getQueryTemplateInputIds(query);
    const inputMatches =
      filters.selectedInputs.size === 0 ||
      inputIds.some((inputId) => filters.selectedInputs.has(inputId));

    const outputIds = getQueryTemplateOutputIds(query);
    const outputMatches =
      filters.selectedOutputs.size === 0 ||
      outputIds.some((outputId) => filters.selectedOutputs.has(outputId));

    return topicMatches && inputMatches && outputMatches;
  });
}
