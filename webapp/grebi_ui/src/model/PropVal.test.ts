import { describe, it, expect } from 'vitest'
import PropVal from './PropVal'

describe('PropVal.language', () => {
  it('knows the language of a translated value', () => {
    const merged = (value: any) => ({ 'grebi:datasources': ['A'], 'grebi:value': value })
    expect(PropVal.from(merged('Gene A')).language()).toBeUndefined()
    expect(PropVal.from(merged({ 'grebi:value': 'Gène A', 'grebi:properties': { 'grebi:lang': ['fr'] } })).language()).toBe('fr')
    expect(PropVal.from(merged({ 'grebi:value': 'Gène A', 'grebi:properties': { 'grebi:lang': ['FR'] } })).language()).toBe('fr')
    expect(PropVal.from(merged({ 'grebi:value': 'x', 'grebi:properties': { 'p': ['q'] } })).language()).toBeUndefined()
    expect(PropVal.from('plain').language()).toBeUndefined()
  })
})

describe('PropVal.from', () => {
  it('wraps primitives with no datasources or props', () => {
    const p = PropVal.from('psoriasis')
    expect(p.value).toBe('psoriasis')
    expect(p.datasources).toEqual([])
    expect(p.props).toEqual({})
    expect(PropVal.from(42).value).toBe(42)
  })

  it('unpacks a datasource-annotated value', () => {
    const p = PropVal.from({ 'grebi:datasources': ['OLS.mondo', 'GWAS'], 'grebi:value': 'psoriasis' })
    expect(p.datasources).toEqual(['OLS.mondo', 'GWAS'])
    expect(p.value).toBe('psoriasis')
    expect(p.props).toEqual({})
  })

  it('unpacks a reified value, keeping the qualifying props alongside', () => {
    const p = PropVal.from({
      'grebi:datasources': ['OLS.hp'],
      'grebi:value': { 'grebi:value': 'hp:0000001', 'grebi:qualifier': 'exact' },
    })
    expect(p.value).toBe('hp:0000001')
    expect(p.props).toEqual({ 'grebi:qualifier': 'exact' })
    expect(p.datasources).toEqual(['OLS.hp'])
  })

  it('keeps an object without both grebi keys as an opaque value', () => {
    const expr = { 'rdf:type': 'owl:Restriction', 'owl:onProperty': 'ro:0002200' }
    expect(PropVal.from(expr).value).toBe(expr)
    expect(PropVal.from({ 'grebi:value': 'x' }).value).toEqual({ 'grebi:value': 'x' })
    expect(PropVal.from({ 'grebi:datasources': ['X'] }).datasources).toEqual([])
  })

  it('returns an existing PropVal untouched', () => {
    const p = new PropVal(['GWAS'], {}, 'x')
    expect(PropVal.from(p)).toBe(p)
  })

  it('treats null and undefined as empty and keeps other falsy values', () => {
    expect(PropVal.from(null).value).toBe('')
    expect(PropVal.from(undefined).value).toBe('')
    expect(PropVal.from(0).value).toBe(0)
    expect(PropVal.from(false).value).toBe(false)
    expect(PropVal.from('').value).toBe('')
  })
})

describe('PropVal.arrFrom and anyFrom', () => {
  it('always yields an array, empty for null and undefined', () => {
    expect(PropVal.arrFrom(undefined)).toEqual([])
    expect(PropVal.arrFrom(null)).toEqual([])
    expect(PropVal.arrFrom('a').map(p => p.value)).toEqual(['a'])
    const arr = PropVal.arrFrom(['a', { 'grebi:datasources': ['X'], 'grebi:value': 'b' }])
    expect(arr.map(p => p.value)).toEqual(['a', 'b'])
    expect(arr[1].datasources).toEqual(['X'])
  })

  it('anyFrom keeps the shape of its input', () => {
    const many = PropVal.anyFrom(['a', 'b'])
    expect(Array.isArray(many)).toBe(true)
    expect((many as PropVal[]).map(p => p.value)).toEqual(['a', 'b'])
    expect(PropVal.anyFrom('a')).toBeInstanceOf(PropVal)
  })
})
