import { describe, it, expect } from 'vitest'
import GraphNodeRef from './GraphNodeRef'

const node = (props: any) => new GraphNodeRef(props)

const merged = (value: any) => ({ 'grebi:datasources': ['A'], 'grebi:value': value })
const translated = (text: string, lang: string) => merged({ 'grebi:value': text, 'grebi:properties': { 'grebi:lang': [lang] } })

describe('GraphNodeRef', () => {
  it('names the node in its own language unless another is asked for', () => {
    const node = new GraphNodeRef({
      'grebi:nodeId': 'ex:a',
      'grebi:name': [translated('Gène A', 'fr'), merged('Gene A'), translated('遺伝子 A', 'ja')],
      'grebi:languages': ['en', 'fr', 'ja'],
    })
    expect(node.getName()).toBe('Gene A')
    expect(node.getName('en')).toBe('Gene A')
    expect(node.getName('fr')).toBe('Gène A')
    expect(node.getName('JA')).toBe('遺伝子 A')
    expect(node.getName('de')).toBe('Gene A')
    expect(node.getLanguages()).toEqual(['en', 'fr', 'ja'])
    expect(new GraphNodeRef({ 'grebi:nodeId': 'ex:b', 'grebi:name': [translated('seulement', 'fr')] }).getName()).toBe('seulement')
    expect(new GraphNodeRef({ 'grebi:nodeId': 'ex:c' }).getLanguages()).toEqual([])
  })

  it('shows the first name, falling back to the id', () => {
    expect(node({ 'grebi:nodeId': 'x', 'grebi:name': ['psoriasis', 'Psoriasis'] }).getName()).toBe('psoriasis')
    expect(node({ 'grebi:nodeId': 'rs162212' }).getName()).toBe('rs162212')
    expect(node({ 'grebi:nodeId': 'x', 'grebi:curie': 'mondo:0005083' }).getName()).toBe('mondo:0005083')
  })

  it('maps known types to a display type, earliest entry winning', () => {
    expect(node({ 'grebi:type': ['gwas:SNP'] }).extractType()?.shortName).toBe('SNP')
    // the GWAS Catalog studies the study-level templates return
    expect(node({ 'grebi:type': ['gwas:Study'] }).extractType()?.longName).toBe('Study')
    expect(node({ 'grebi:type': ['metabolights:Study'] }).extractType()?.longName).toBe('Study')
    // biolink:Gene is listed before ols:Class, so a gene that is also a class shows as a gene
    expect(node({ 'grebi:type': ['ols:Class', 'biolink:Gene'] }).extractType()?.shortName).toBe('Gene')
  })

  it('has no display type for unknown types', () => {
    expect(node({ 'grebi:type': ['otar:Evidence'] }).extractType()).toBeUndefined()
    expect(node({}).extractType()).toBeUndefined()
  })

  it('refuses to wrap nothing', () => {
    expect(() => new GraphNodeRef(null)).toThrow()
  })
})
