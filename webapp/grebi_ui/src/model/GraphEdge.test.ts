import { describe, it, expect } from 'vitest'
import GraphEdge from './GraphEdge'
import GraphNodeRef from './GraphNodeRef'

const edge = () => new GraphEdge({
  'grebi:edgeId': 'edge-1',
  'grebi:type': 'biolink:has_phenotype',
  'grebi:datasources': ['OLS.hp', 'Monarch'],
  from: { 'grebi:nodeId': 'mondo:0005083', 'grebi:name': ['psoriasis'] },
  to: { 'grebi:nodeId': 'hp:0000001', 'grebi:name': ['phenotypic abnormality'] },
})

describe('GraphEdge', () => {
  it('exposes id, type and datasources', () => {
    expect(edge().getEdgeId()).toBe('edge-1')
    expect(edge().getType()).toBe('biolink:has_phenotype')
    expect(edge().getDatasources()).toEqual(['OLS.hp', 'Monarch'])
  })

  it('wraps both endpoints as node refs', () => {
    const from = edge().getFrom()
    const to = edge().getTo()
    expect(from).toBeInstanceOf(GraphNodeRef)
    expect(from.getNodeId()).toBe('mondo:0005083')
    expect(from.getName()).toBe('psoriasis')
    expect(to.getName()).toBe('phenotypic abnormality')
    expect(to.getEncodedNodeId()).toBe('aHA6MDAwMDAwMQ')
  })

  it('refuses to build a ref for a missing endpoint', () => {
    const e = new GraphEdge({ 'grebi:edgeId': 'e', from: { 'grebi:nodeId': 'a' } })
    expect(e.getFrom().getNodeId()).toBe('a')
    expect(() => e.getTo()).toThrow()
  })
})
