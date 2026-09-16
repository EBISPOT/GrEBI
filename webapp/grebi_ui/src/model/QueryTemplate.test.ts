import { describe, it, expect } from 'vitest'
import { paramSuggestFilters, Parameter } from './QueryTemplate'

const param = (extra: Partial<Parameter> = {}): Parameter =>
  ({ param_id: 'cell_type_id', param_name: 'Cell Type', param_type: 'SourceId', ...extra })

describe('paramSuggestFilters', () => {
  it('narrows to descendants for values_under', () => {
    const filters = paramSuggestFilters(param({ values_under: 'cl:0000000' }))
    expect(filters?.get('biolink:broad_match')).toBe('cl:0000000')
    expect(filters?.toString()).toBe('biolink%3Abroad_match=cl%3A0000000')
  })

  it('narrows to a type label for values_with_type', () => {
    const filters = paramSuggestFilters(param({ values_with_type: 'gwas:Study' }))
    expect(filters?.get('grebi:type')).toBe('gwas:Study')
    expect(Array.from(filters!.keys())).toEqual(['grebi:type'])
  })

  it('is unconstrained when neither is set', () => {
    expect(paramSuggestFilters(param())).toBeUndefined()
    expect(paramSuggestFilters(param({ values_under: '', values_with_type: '' }))).toBeUndefined()
  })

  it('prefers values_under when both are given', () => {
    const filters = paramSuggestFilters(param({ values_under: 'cl:0000000', values_with_type: 'gwas:Study' }))
    expect(filters?.has('biolink:broad_match')).toBe(true)
    expect(filters?.has('grebi:type')).toBe(false)
  })
})
