import { describe, it, expect } from 'vitest'
import {
  getQueryTemplateInputIds, getQueryTemplateOutputIds,
  getAvailableQueryTemplateInputs, getAvailableQueryTemplateOutputs,
  filterQueryTemplates, QueryTemplateFilters,
} from './queryTemplateFilters'

const t = (id: string, topics?: string[], inputs?: string[], outputs?: string[]): any => ({
  id,
  topics,
  params: inputs?.map(param_id => ({ param_id })),
  result_columns: outputs?.map(column_id => ({ column_id })),
})

const a = t('a', ['gwas'], ['trait_id'], ['study', 'trait'])
const b = t('b', ['gwas', 'impc'], ['gene_id'], ['study'])
const c = t('c', [], [], ['gene'])
const d = t('d')
const queries = [a, b, c, d]

const filters = (f: Partial<{ [K in keyof QueryTemplateFilters]: string[] }> = {}): QueryTemplateFilters => ({
  selectedTopics: new Set(f.selectedTopics || []),
  selectedInputs: new Set(f.selectedInputs || []),
  selectedOutputs: new Set(f.selectedOutputs || []),
})
const ids = (qs: any[]) => qs.map(q => q.id)

describe('query template ids', () => {
  it('reads input ids from params and output ids from result columns', () => {
    expect(getQueryTemplateInputIds(a)).toEqual(['trait_id'])
    expect(getQueryTemplateOutputIds(a)).toEqual(['study', 'trait'])
    expect(getQueryTemplateInputIds(d)).toEqual([])
    expect(getQueryTemplateOutputIds(d)).toEqual([])
  })

  it('lists the distinct ids across templates in alphabetical order', () => {
    expect(getAvailableQueryTemplateInputs(queries)).toEqual(['gene_id', 'trait_id'])
    expect(getAvailableQueryTemplateOutputs(queries)).toEqual(['gene', 'study', 'trait'])
    expect(getAvailableQueryTemplateInputs([])).toEqual([])
  })
})

describe('filterQueryTemplates', () => {
  it('keeps everything when nothing is selected', () => {
    expect(ids(filterQueryTemplates(queries, filters()))).toEqual(['a', 'b', 'c', 'd'])
  })

  it('matches any selected value within a dimension', () => {
    expect(ids(filterQueryTemplates(queries, filters({ selectedTopics: ['impc'] })))).toEqual(['b'])
    expect(ids(filterQueryTemplates(queries, filters({ selectedInputs: ['trait_id', 'gene_id'] })))).toEqual(['a', 'b'])
    expect(ids(filterQueryTemplates(queries, filters({ selectedOutputs: ['gene'] })))).toEqual(['c'])
  })

  it('requires every dimension to match', () => {
    expect(ids(filterQueryTemplates(queries, filters({ selectedTopics: ['gwas'], selectedOutputs: ['trait'] })))).toEqual(['a'])
    expect(ids(filterQueryTemplates(queries, filters({ selectedTopics: ['gwas'], selectedInputs: ['nope'] })))).toEqual([])
  })

  it('never matches a template that lacks the filtered dimension', () => {
    expect(ids(filterQueryTemplates([c, d], filters({ selectedTopics: ['gwas'] })))).toEqual([])
    expect(ids(filterQueryTemplates([c, d], filters({ selectedInputs: ['trait_id'] })))).toEqual([])
  })
})
