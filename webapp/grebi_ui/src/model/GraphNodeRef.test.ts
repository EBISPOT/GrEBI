import { describe, it, expect } from 'vitest'
import GraphNodeRef from './GraphNodeRef'

const node = (props: any) => new GraphNodeRef(props)

describe('GraphNodeRef', () => {
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
