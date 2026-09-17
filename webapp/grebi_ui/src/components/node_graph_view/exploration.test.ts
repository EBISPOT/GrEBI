import { describe, it, expect } from 'vitest'
import GraphNodeRef from '../../model/GraphNodeRef'
import { explorationOf, serialiseExploration, parseExploration, isEmptyExploration } from './exploration'
import { expandedKey } from './useGraphViewState'

const ref = (id: string) => new GraphNodeRef({ 'grebi:nodeId': id, 'grebi:name': [id] })
const expansion = (parent: string, direction: 'incoming' | 'outgoing', edgeType: string, child: string) =>
  [expandedKey(parent, direction, edgeType), { node: ref(child), edgeType, direction, parentNodeId: parent, incomingEdgeCounts: {}, outgoingEdgeCounts: {}, loading: false }] as const

describe('exploration', () => {
  it('reads the chain of expansions from the root, and the filters sorted', () => {
    const expanded = new Map<string, any>([
      expansion('child', 'incoming', 'regulates', 'grandchild'),
      expansion('root', 'outgoing', 'has_part', 'child'),
      expansion('orphan', 'outgoing', 'x', 'nowhere'),
    ])
    const e = explorationOf('root', expanded, new Set(['ds2', 'ds1']), new Set(['b', 'a']))
    expect(e.steps).toEqual([
      { direction: 'outgoing', edgeType: 'has_part', nodeId: 'child' },
      { direction: 'incoming', edgeType: 'regulates', nodeId: 'grandchild' },
    ])
    expect(e.excludedDatasources).toEqual(['ds1', 'ds2'])
    expect(e.hiddenEdgeTypes).toEqual(['a', 'b'])
  })

  it('serialises to a URL-safe token and back, and to nothing when empty', () => {
    const e = {
      steps: [{ direction: 'outgoing' as const, edgeType: 'biolink:has_part', nodeId: 'mondo:0005083' }, { direction: 'incoming' as const, edgeType: 'rdfs:label', nodeId: 'http://example.org/Gène?x=1&y=2' }],
      excludedDatasources: ['OLS.mondo'],
      hiddenEdgeTypes: [],
    }
    const token = serialiseExploration(e)!
    expect(token).toMatch(/^[A-Za-z0-9_-]+$/)
    expect(parseExploration(token)).toEqual(e)
    expect(serialiseExploration({ steps: [], excludedDatasources: [], hiddenEdgeTypes: [] })).toBeNull()
    expect(isEmptyExploration(parseExploration(serialiseExploration({ steps: [], excludedDatasources: ['x'], hiddenEdgeTypes: [] }))!)).toBe(false)
  })

  it('refuses tokens that are not explorations', () => {
    expect(parseExploration(null)).toBeNull()
    expect(parseExploration('')).toBeNull()
    expect(parseExploration('not base64 json!')).toBeNull()
    expect(parseExploration(btoa('"a string"'))).toBeNull()
    expect(parseExploration(btoa('{"s":[["sideways","t","n"]]}'))).toBeNull()
    expect(parseExploration(btoa('{"x":"not a list"}'))).toBeNull()
    expect(parseExploration(btoa('{}'))).toEqual({ steps: [], excludedDatasources: [], hiddenEdgeTypes: [] })
  })
})
