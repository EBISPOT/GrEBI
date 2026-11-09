import { describe, it, expect } from 'vitest'
import { computeRadialLayout, groupStems, GraphLayout, LayoutNode, StemGrouping, viewBBox } from './graphLayout'
import { AggregatedEdgeCount, CategoryOpening, ExpandedNodeState, categoryKey, expandedKey } from './useGraphViewState'
import { parseGraphViewConfig } from './graphViewConfig'
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
  grouping?: StemGrouping
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
    opts.auto || new Map(),
    opts.grouping
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

  it('merges count=1 edges that resolve to the same node into one real node, reached by one stem for all their types', () => {
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
    // one stem, not an edge per type: parallel edges swept across the neighbouring stems
    const toN1 = l.edges.filter((e) => e.target === 'n1')
    expect(toN1).toHaveLength(1)
    expect(toN1[0]).toMatchObject({ source: 'root', type: 'expanded', dashed: false, label: 'a, b', edgeType: 'a', edgeTypes: ['a', 'b'], datasources: ['ds1', 'ds2'] })
    expect(byId(l, 'n1')).toMatchObject({ edgeTypes: ['a', 'b'] })
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

  it('keeps bubbles apart and is deterministic', () => {
    // four equally-sized (60px) bubbles at even angles on the outgoing arc of the
    // usual ring would be closer than their combined radii plus padding
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

describe('where the stems and their labels go', () => {
  // the padding the layout keeps between bubbles
  const PAD = 15
  const stems = (l: GraphLayout) => l.nodes.filter((n) => n.type !== 'root')
  /** No two bubbles nearer than the padding, and, on one side of the root, each label with a row of its own clear of the other bubbles. */
  function expectNothingToClash(l: GraphLayout) {
    const nodes = stems(l)
    for (let i = 0; i < nodes.length; i++) {
      for (let j = i + 1; j < nodes.length; j++) {
        const a = nodes[i], b = nodes[j]
        expect(dist(a, b), `${a.id} and ${b.id} overlap`).toBeGreaterThanOrEqual(a.size + b.size + PAD - 1e-6)
        if (a.outerLabelSide === b.outerLabelSide) {
          // the label of the one nearer the root's axis runs past the other bubble
          const outer = Math.abs(a.x) > Math.abs(b.x) ? a : b
          expect(Math.abs(a.y - b.y), `the labels of ${a.id} and ${b.id}`).toBeGreaterThanOrEqual(Math.max(a.rowGap!, outer.size + a.rowGap! / 2) - 1e-6)
        }
      }
    }
  }
  const ring = (l: GraphLayout) => Math.max(...stems(l).map((n) => Math.hypot(n.x, n.y)))
  const many = (n: number, prefix: string) => Array.from({ length: n }, (_, i) => agg(`${prefix}:${i}`, { ds: 1000 - 37 * i }))

  it('labels each stem beside its bubble, on the side away from the root, and not along the stem', () => {
    const l = layout({ incoming: [agg('is_a', { ols: 5 })], outgoing: [agg('has_part', { ols: 3 })] })
    expect(byId(l, 'count::root::incoming::is_a')).toMatchObject({ outerLabel: 'is_a', outerLabelSide: 'left' })
    expect(byId(l, 'count::root::outgoing::has_part')).toMatchObject({ outerLabel: 'has_part', outerLabelSide: 'right' })
    expect(l.edges.map((e) => e.labelled)).toEqual([false, false])
    // the root is named under its circle when the name does not fit inside
    expect(byId(l, 'root')).toMatchObject({ namePlacement: 'below' })
    expect(byId(l, 'root')!.outerLabel).toBeUndefined()
  })

  it('says in the label of a node a single edge leads to which node it is, and which way the edge runs', () => {
    const auto = new Map<string, GraphNodeRef>([
      [expandedKey('root', 'outgoing', 'has_parent'), ref('p', 'The parent')],
      [expandedKey('root', 'incoming', 'has_parent'), ref('c', 'A child')],
    ])
    const l = layout({ incoming: [agg('has_parent', { ds: 1 })], outgoing: [agg('has_parent', { ds: 1 })], auto })
    expect(byId(l, 'p')).toMatchObject({ outerLabel: 'has_parent \u2192 The parent', outerLabelSide: 'right' })
    expect(byId(l, 'c')).toMatchObject({ outerLabel: 'A child \u2192 has_parent', outerLabelSide: 'left' })
  })

  it('keeps a few stems on the usual ring, spread over their side, in order from the top down', () => {
    const l = layout({ incoming: many(3, 'in'), outgoing: many(4, 'out') })
    expect(ring(l)).toBeCloseTo(250)
    expectNothingToClash(l)
    for (const side of ['in', 'out']) {
      const ys = stems(l).filter((n) => n.id.includes(`::${side}:`)).map((n) => n.y)
      expect(ys).toEqual([...ys].sort((a, b) => b - a))
      // spread out, not bunched at the top
      expect(Math.min(...ys)).toBeLessThan(-50)
    }
  })

  it('grows the ring for a busy node until every bubble and every label is clear of the others', () => {
    const l = layout({ incoming: many(9, 'in'), outgoing: many(14, 'out') })
    expect(stems(l)).toHaveLength(23)
    expect(ring(l)).toBeGreaterThan(300)
    expectNothingToClash(l)
    // both sides are on the one ring
    for (const n of stems(l)) expect(Math.hypot(n.x, n.y)).toBeCloseTo(ring(l))
    expect(layout({ incoming: many(9, 'in'), outgoing: many(14, 'out') })).toEqual(l)
  })

  it('still fits a node with more stems than a view has rows for labels, with shorter rows', () => {
    const l = layout({ outgoing: many(60, 'out') })
    expect(stems(l)).toHaveLength(60)
    expect(ring(l)).toBeLessThan(3000)
    expectNothingToClash(l)
    // the height kept for each label stops growing with the ring
    expect(new Set(stems(l).map((n) => n.rowGap))).toEqual(new Set([44]))
  })

  it('lists an expanded node\'s stems from the top down on each half of its cone, and names the chain alternately above and below', () => {
    const counts = Object.fromEntries(many(4, 'c').map((a) => [a.edgeType, a.dsToCount]))
    const expanded = new Map<string, ExpandedNodeState>([
      expansion('root', 'outgoing', 'has_part', ref('child', 'Child'), { incoming: counts, outgoing: counts }),
      expansion('child', 'outgoing', 'c:0', ref('grandchild', 'Grandchild')),
    ])
    const one = layout({ outgoing: [agg('has_part', { ds1: 10 })], expanded: new Map([...expanded].slice(0, 1)) })
    for (const direction of ['incoming', 'outgoing']) {
      const ys = one.nodes.filter((n) => n.id.startsWith(`count::child::${direction}`)).map((n) => n.y)
      expect(ys).toHaveLength(4)
      expect(ys).toEqual([...ys].sort((a, b) => b - a))
    }
    // outgoing above the expanded node, incoming below it
    expect(byId(one, 'count::child::outgoing::c:0')!.y).toBeGreaterThan(0)
    expect(byId(one, 'count::child::incoming::c:0')!.y).toBeLessThan(0)
    // the edge of an expansion is alone between its nodes and keeps its label along it
    expect(one.edges.find((e) => e.id === 'edge::root::outgoing::has_part')).toMatchObject({ labelled: true, label: 'has_part' })

    const two = layout({ outgoing: [agg('has_part', { ds1: 10 })], expanded })
    expect(byId(two, 'root')!.namePlacement).toBe('below')
    expect(byId(two, 'child')!.namePlacement).toBe('above')
    expect(byId(two, 'grandchild')!.namePlacement).toBe('below')
  })
})

describe('viewBBox', () => {
  const at = (id: string, x: number, y: number): LayoutNode =>
    ({ id, x, y, size: 28, label: id, type: 'auto_expanded_node', color: '#4a90d9', labelColor: '#fff' })
  const expectBox = (box: ReturnType<typeof viewBBox>, x: [number, number], y: [number, number]) => {
    expect(box).not.toBeNull()
    box!.x.forEach((v, i) => expect(v).toBeCloseTo(x[i]))
    box!.y.forEach((v, i) => expect(v).toBeCloseTo(y[i]))
  }

  it('gives the root on its own (every edge type hidden) a box the size of the ring of stems', () => {
    expect(viewBBox(layout())).toEqual({ x: [-250, 250], y: [-250, 250] })
  })

  it('centres the box on the layout when only a few stems are left, with room for their labels', () => {
    // one outgoing stem, straight out to the right of the root: the ring's size around
    // the two nodes, and beside it, on the side the label is on, 45% of the box's height
    expectBox(viewBBox(layout({ outgoing: [agg('a', { ds1: 3 })] })), [-125, 375 + 225], [-250, 250])
  })

  it('keeps the ring\'s height for a layout that is wide but flat', () => {
    // one incoming and one outgoing stem: as wide as the ring, with no height, labelled on both sides
    expectBox(viewBBox(layout({ incoming: [agg('a', { ds1: 3 })], outgoing: [agg('a', { ds1: 5 })] })), [-250 - 225, 250 + 225], [-250, 250])
    // wider than the ring: keeps its own width
    expectBox(viewBBox({ nodes: [at('l', -400, 0), at('r', 400, 50)], edges: [] }), [-400, 400], [-225, 275])
  })

  it('lets a layout larger than the ring both ways be fitted as it is, unless it has labels beside it', () => {
    const a = at('a', -300, -300), b = at('b', 300, 300)
    expect(viewBBox({ nodes: [a, b], edges: [] })).toBeNull()
    expect(viewBBox({ nodes: [], edges: [] })).toBeNull()
    // labelled on the left only: room on the left only, 45% of the height of 600
    expectBox(viewBBox({ nodes: [{ ...a, outerLabel: 'is_a', outerLabelSide: 'left' }, b], edges: [] }), [-300 - 270, 300], [-300, 300])
  })
})

describe('grouping stems by category', () => {
  const config = parseGraphViewConfig({
    max_stems: 4,
    pinned_edge_types: ['biolink:subclass_of'],
    categories: [
      { id: 'hierarchy', label: 'Hierarchy', edge_types: ['biolink:subclass_of', '*:parent'] },
      { id: 'assoc', label: 'Associations', edge_types: ['*association', 'biolink:affects'] },
      { id: 'parts', label: 'Parts', edge_types: ['*part*'] },
    ],
  })
  const grouping = (open: [string, CategoryOpening][] = []): StemGrouping => ({ config, openCategories: new Map(open) })
  const describeGroups = (groups: ReturnType<typeof groupStems>) =>
    groups.map((g) => (g.kind === 'stem' ? g.agg.edgeType : `${g.kind}:${g.category.id}(${g.aggs.map((a) => a.edgeType).join(',')})${g.kind === 'category' && g.opening ? '/' + g.opening : ''}`))

  const outgoing = [
    agg('z:one', { d: 3 }),
    agg('biolink:affects', { d: 10 }),
    agg('x:parent', { d: 1 }),
    agg('a_association', { d: 100 }),
    agg('biolink:subclass_of', { d: 2 }),
    agg('biolink:has_part', { d: 5 }),
    agg('b_association', { d: 50 }),
    agg('z:two', { d: 4 }),
  ]

  it('draws every stem, in the categories\' order and largest first, when there are no more than max_stems', () => {
    expect(describeGroups(groupStems(outgoing, grouping(), false, 'root', 'outgoing'))).toEqual([
      'biolink:subclass_of', 'x:parent', 'a_association', 'b_association', 'biolink:affects', 'biolink:has_part', 'z:two', 'z:one',
    ])
    // without grouping, as the counts came
    expect(describeGroups(groupStems(outgoing, undefined, false, 'root', 'outgoing'))).toEqual(outgoing.map((a) => a.edgeType))
  })

  it('gathers each category into a bubble, but not pinned edge types or a category with one stem', () => {
    expect(describeGroups(groupStems(outgoing, grouping(), true, 'root', 'outgoing'))).toEqual([
      'biolink:subclass_of', 'x:parent', 'category:assoc(a_association,b_association,biolink:affects)', 'biolink:has_part', 'category:other(z:two,z:one)',
    ])
  })

  it('opens a category to its largest max_stems stems and a "+N more", then to all of them', () => {
    const many = ['a', 'b', 'c', 'd', 'e', 'f'].map((x, i) => agg(`${x}_association`, { d: 60 - i }))
    const key = categoryKey('root', 'outgoing', 'assoc')
    expect(describeGroups(groupStems(many, grouping([[key, 'open']]), true, 'root', 'outgoing'))).toEqual([
      'category:assoc(a_association,b_association,c_association,d_association,e_association,f_association)/open',
      'a_association', 'b_association', 'c_association', 'd_association',
      'more:assoc(e_association,f_association)',
    ])
    expect(describeGroups(groupStems(many, grouping([[key, 'all']]), true, 'root', 'outgoing')).slice(1)).toEqual(many.map((a) => a.edgeType))
    // opened around another parent or in the other direction, it stays gathered
    expect(groupStems(many, grouping([[categoryKey('root', 'incoming', 'assoc'), 'all']]), true, 'root', 'outgoing')).toHaveLength(1)
  })

  it('counts both directions of the root against max_stems, and lays out category bubbles with their stems', () => {
    // 3 + 2 stems is more than 4: gathered
    const l = layout({
      incoming: [agg('p_association', { ds1: 7 }), agg('q_association', { ds2: 8 }), agg('r:one', { ds1: 1 })],
      outgoing: [agg('a_association', { ds1: 100 }), agg('b_association', { ds1: 50, ds2: 5 })],
      grouping: grouping(),
    })
    expect(byId(l, 'category::root::outgoing::assoc')).toMatchObject({
      type: 'category', label: '155', count: 155, open: false, categoryId: 'assoc', categoryLabel: 'Associations',
      edgeTypes: ['a_association', 'b_association'], datasources: ['ds1', 'ds2'], dsToCount: { ds1: 150, ds2: 5 },
      direction: 'outgoing', parentNodeId: 'root',
    })
    expect(byId(l, 'count::root::outgoing::a_association')).toBeUndefined()
    expect(l.edges.find((e) => e.target === 'category::root::outgoing::assoc')).toMatchObject({
      source: 'root', label: 'Associations \u00b7 2 types', dashed: true, edgeTypes: ['a_association', 'b_association'],
    })
    // incoming: the stem runs from the bubble to the root
    expect(l.edges.find((e) => e.source === 'category::root::incoming::assoc')).toMatchObject({ target: 'root' })
    // the lone stem in "other" is drawn as itself
    expect(byId(l, 'count::root::incoming::r:one')).toBeDefined()

    // fewer stems than max_stems: no bubbles
    const small = layout({ outgoing: [agg('a_association', { ds1: 100 }), agg('b_association', { ds1: 50 })], grouping: grouping() })
    expect(small.nodes.some((n) => n.type === 'category')).toBe(false)
  })

  it('draws an open category as a small bubble that gathers its stems back, with a "+N more" for the rest', () => {
    const many = ['a', 'b', 'c', 'd', 'e', 'f'].map((x, i) => agg(`${x}_association`, { d: 60 - i }))
    const l = layout({ outgoing: many, grouping: grouping([[categoryKey('root', 'outgoing', 'assoc'), 'open']]) })
    expect(byId(l, 'category::root::outgoing::assoc')).toMatchObject({ open: true, label: '\u2212', size: 16 })
    expect(l.nodes.filter((n) => n.type === 'count').map((n) => n.edgeType)).toEqual(['a_association', 'b_association', 'c_association', 'd_association'])
    expect(byId(l, 'more::root::outgoing::assoc')).toMatchObject({ type: 'more', label: '+2', count: 111, edgeTypes: ['e_association', 'f_association'], categoryId: 'assoc' })
    expect(l.edges.find((e) => e.target === 'more::root::outgoing::assoc')).toMatchObject({ label: '2 more' })
  })

  it('gathers an expanded node\'s children by its own count', () => {
    const counts = Object.fromEntries(['a', 'b', 'c', 'd', 'e'].map((x) => [`${x}_association`, { ds1: 2 }]))
    const expanded = new Map<string, ExpandedNodeState>([expansion('root', 'outgoing', 'has_part', ref('child', 'Child'), { outgoing: counts })])
    const l = layout({ outgoing: [agg('has_part', { ds1: 10 })], expanded, grouping: grouping() })
    expect(byId(l, 'category::child::outgoing::assoc')).toMatchObject({ parentNodeId: 'child', edgeTypes: Object.keys(counts) })
  })
})
