import { describe, it, expect } from 'vitest'
import { computeRadialLayout, GraphLayout, LayoutNode } from './graphLayout'
import { AggregatedEdgeCount, ExpandedNodeState, expandedKey } from './useGraphViewState'
import GraphNodeRef from '../../model/GraphNodeRef'
import encodeNodeId from '../../encodeNodeId'

const ref = (id: string, name?: string) =>
  new GraphNodeRef({ 'grebi:nodeId': id, ...(name ? { 'grebi:name': [name] } : {}) })

function agg(edgeType: string, dsToCount: Record<string, number>): AggregatedEdgeCount {
  return {
    edgeType,
    datasources: Object.keys(dsToCount),
    totalCount: Object.values(dsToCount).reduce((a, b) => a + b, 0),
    dsToCount,
  }
}

function expansion(
  parentNodeId: string,
  direction: 'incoming' | 'outgoing',
  edgeType: string,
  node: GraphNodeRef,
  counts: { incoming?: any; outgoing?: any } = {},
  loading = false
): [string, ExpandedNodeState] {
  return [
    expandedKey(parentNodeId, direction, edgeType),
    {
      node,
      edgeType,
      direction,
      parentNodeId,
      incomingEdgeCounts: counts.incoming || {},
      outgoingEdgeCounts: counts.outgoing || {},
      loading,
    },
  ]
}

function layout(opts: {
  incoming?: AggregatedEdgeCount[]
  outgoing?: AggregatedEdgeCount[]
  expanded?: Map<string, ExpandedNodeState>
  auto?: Map<string, GraphNodeRef>
  dsExclude?: Set<string>
  hidden?: Set<string>
} = {}): GraphLayout {
  return computeRadialLayout(
    'root',
    'Root node',
    encodeNodeId('root'),
    opts.incoming || [],
    opts.outgoing || [],
    opts.expanded || new Map(),
    opts.dsExclude || new Set(),
    opts.hidden || new Set(),
    opts.auto || new Map()
  )
}

const byId = (l: GraphLayout, id: string) => l.nodes.find((n) => n.id === id)
const dist = (a: LayoutNode, b: LayoutNode) => Math.hypot(a.x - b.x, a.y - b.y)

describe('computeRadialLayout', () => {
  it('lays out only the root at the origin when there are no edges', () => {
    const l = layout()
    expect(l.nodes).toHaveLength(1)
    expect(l.nodes[0]).toMatchObject({ id: 'root', x: 0, y: 0, type: 'root', label: 'Root node', hasHiddenChildren: false })
    expect(l.edges).toEqual([])
  })

  it('puts incoming counts on the left and outgoing counts on the right, with stems pointing the right way', () => {
    const l = layout({ incoming: [agg('is_a', { ols: 5 })], outgoing: [agg('has_part', { ols: 3 })] })

    const inc = byId(l, 'count::root::incoming::is_a')!
    const out = byId(l, 'count::root::outgoing::has_part')!
    expect(inc.x).toBeCloseTo(-250)
    expect(inc.y).toBeCloseTo(0)
    expect(out.x).toBeCloseTo(250)
    expect(out.y).toBeCloseTo(0)
    expect(inc).toMatchObject({
      type: 'count', direction: 'incoming', edgeType: 'is_a', count: 5, label: '5',
      datasources: ['ols'], dsToCount: { ols: 5 }, parentNodeId: 'root', parentEncodedNodeId: encodeNodeId('root'),
    })

    // stems are dashed; incoming stems run count -> root, outgoing stems root -> count
    expect(l.edges).toHaveLength(2)
    expect(l.edges.find((e) => e.id === 'edge::root::incoming::is_a')).toMatchObject({
      source: 'count::root::incoming::is_a', target: 'root', type: 'stem', dashed: true, label: 'is_a', direction: 'incoming',
    })
    expect(l.edges.find((e) => e.id === 'edge::root::outgoing::has_part')).toMatchObject({
      source: 'root', target: 'count::root::outgoing::has_part', type: 'stem', dashed: true, label: 'has_part',
    })
  })

  it('sizes count bubbles relative to the largest count and formats labels compactly', () => {
    const l = layout({ outgoing: [agg('big', { a: 1500 }), agg('small', { a: 10 })] })
    const big = byId(l, 'count::root::outgoing::big')!
    const small = byId(l, 'count::root::outgoing::small')!
    expect(big.size).toBe(60)
    expect(small.size).toBeCloseTo(20 + (10 / 1500) * 40)
    expect(big.label).toBe('1.5K')
    expect(small.label).toBe('10')
  })

  it('merges count=1 edges that resolve to the same node into one real node with one edge per edge type', () => {
    const target = ref('n1', 'Node One')
    const auto = new Map<string, GraphNodeRef>([
      [expandedKey('root', 'outgoing', 'a'), target],
      [expandedKey('root', 'outgoing', 'b'), target],
    ])
    const l = layout({
      outgoing: [agg('a', { ds1: 1 }), agg('b', { ds2: 1 }), agg('c', { ds3: 1 })],
      auto,
    })

    expect(l.nodes.map((n) => n.id).sort()).toEqual(['count::root::outgoing::c', 'n1', 'root'])
    expect(byId(l, 'n1')).toMatchObject({
      type: 'auto_expanded_node', label: 'Node One', count: 1, edgeType: 'a',
      datasources: ['ds1', 'ds2'], dsToCount: { ds1: 1, ds2: 1 }, parentNodeId: 'root',
    })
    const toN1 = l.edges.filter((e) => e.target === 'n1')
    expect(toN1.map((e) => e.label).sort()).toEqual(['a', 'b'])
    expect(toN1.every((e) => e.source === 'root' && e.type === 'expanded' && !e.dashed)).toBe(true)
    // a count=1 edge that has not been resolved yet stays a count bubble
    expect(byId(l, 'count::root::outgoing::c')?.type).toBe('count')
  })

  it('shows only the expanded edge of an expanded parent and lays out the expanded node\'s own children', () => {
    const child = ref('child', 'Child')
    const child2 = ref('child2', 'Child Two')
    const expanded = new Map<string, ExpandedNodeState>([
      expansion('root', 'outgoing', 'a', child, { outgoing: { c: { ds1: 4 } } }),
      expansion('root', 'incoming', 'is_a', child2),
    ])
    const l = layout({
      incoming: [agg('is_a', { ols: 5 })],
      outgoing: [agg('a', { ds1: 10 }), agg('b', { ds1: 20 })],
      expanded,
    })

    // the sibling edge type 'b' is hidden while 'a' is expanded
    expect(byId(l, 'count::root::outgoing::b')).toBeUndefined()
    expect(byId(l, 'root')?.hasHiddenChildren).toBe(true)

    const childNode = byId(l, 'child')!
    expect(childNode).toMatchObject({ type: 'expanded_node', label: 'Child', direction: 'outgoing', edgeType: 'a', parentNodeId: 'root', hasHiddenChildren: false })
    expect(childNode.x).toBeCloseTo(250)
    expect(childNode.y).toBeCloseTo(0)
    expect(l.edges.find((e) => e.id === 'edge::root::outgoing::a')).toMatchObject({ source: 'root', target: 'child', type: 'expanded', dashed: false })

    // the expanded node's children hang off it, attributed to it (not the root)
    expect(byId(l, 'count::child::outgoing::c')).toMatchObject({
      type: 'count', count: 4, parentNodeId: 'child', parentEncodedNodeId: encodeNodeId('child'),
    })
    expect(l.edges.find((e) => e.id === 'edge::child::outgoing::c')).toMatchObject({ source: 'child', target: 'count::child::outgoing::c', type: 'stem' })

    // an incoming expansion draws its edge into the parent
    expect(l.edges.find((e) => e.id === 'edge::root::incoming::is_a')).toMatchObject({ source: 'child2', target: 'root', type: 'expanded' })
  })

  it('does not lay out children of an expansion that is still loading', () => {
    const expanded = new Map<string, ExpandedNodeState>([
      expansion('root', 'outgoing', 'a', ref('child', 'Child'), { outgoing: { c: { ds1: 4 } } }, true),
    ])
    const l = layout({ outgoing: [agg('a', { ds1: 10 })], expanded })
    expect(byId(l, 'child')).toBeDefined()
    expect(l.nodes.some((n) => n.id.startsWith('count::child'))).toBe(false)
  })

  it('applies hidden edge types and excluded datasources to an expanded node\'s children', () => {
    const expanded = new Map<string, ExpandedNodeState>([
      expansion('root', 'outgoing', 'a', ref('child', 'Child'), {
        outgoing: { c: { ds1: 4, ds2: 2 }, d: { ds1: 1 }, e: { ds2: 9 } },
      }),
    ])
    const l = layout({
      outgoing: [agg('a', { ds1: 10 })],
      expanded,
      hidden: new Set(['d']),
      dsExclude: new Set(['ds2']),
    })
    const childCounts = l.nodes.filter((n) => n.id.startsWith('count::child'))
    expect(childCounts.map((n) => n.id)).toEqual(['count::child::outgoing::c'])
    expect(childCounts[0]).toMatchObject({ count: 4, label: '4' })
  })

  it('skips resolved nodes that are already on the canvas, such as an edge pointing back at the root', () => {
    const auto = new Map([[expandedKey('root', 'outgoing', 'self'), ref('root', 'Root node')]])
    const l = layout({ outgoing: [agg('self', { ds1: 1 })], auto })
    expect(l.nodes).toHaveLength(1)
    expect(l.edges).toEqual([])
  })

  it('pushes overlapping bubbles apart and is deterministic', () => {
    // four equally-sized (60px) bubbles on the outgoing arc are closer than
    // their combined radii plus padding, so collision resolution must separate them
    const opts = { outgoing: ['a', 'b', 'c', 'd'].map((t) => agg(t, { ds: 100 })) }
    const l = layout(opts)
    const bubbles = l.nodes.filter((n) => n.type === 'count')
    expect(bubbles).toHaveLength(4)
    for (let i = 0; i < bubbles.length; i++) {
      for (let j = i + 1; j < bubbles.length; j++) {
        expect(dist(bubbles[i], bubbles[j])).toBeGreaterThanOrEqual(60 + 60 + 15 - 1e-6)
      }
    }
    // root never moves
    expect(byId(l, 'root')).toMatchObject({ x: 0, y: 0 })
    expect(layout(opts)).toEqual(l)
  })
})
