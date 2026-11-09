import { describe, it, expect } from 'vitest'
import { nodePath, NODE_MAX_VALUES } from './nodePath'
import encodeNodeId from '../encodeNodeId'

describe('nodePath', () => {
  const enc = encodeNodeId('mondo:0005083')

  it('asks for the node abridged unless the whole node is wanted', () => {
    expect(NODE_MAX_VALUES).toBe(100)
    expect(nodePath('g1', 'mondo:0005083')).toBe(`api/v1/graphs/g1/nodes/${enc}?maxValues=100`)
    expect(nodePath('g1', 'mondo:0005083', { whole: true })).toBe(`api/v1/graphs/g1/nodes/${enc}`)
  })

  it('passes the language on', () => {
    expect(nodePath('g1', 'mondo:0005083', { lang: 'fr' })).toBe(`api/v1/graphs/g1/nodes/${enc}?lang=fr&maxValues=100`)
    expect(nodePath('g1', 'mondo:0005083', { lang: 'zh hant', whole: true })).toBe(`api/v1/graphs/g1/nodes/${enc}?lang=zh%20hant`)
  })
})
