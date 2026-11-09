import { describe, it, expect, vi, beforeEach } from 'vitest'
import { renderHook, act, waitFor } from '@testing-library/react'
import GraphNodeRef from '../../model/GraphNodeRef'

vi.mock('../../app/api', () => ({ post: vi.fn() }))
vi.mock('./edgeCountsCache', () => ({ fetchNodeEdgeCounts: vi.fn(), prefetchNodeEdgeCounts: vi.fn() }))

import { post } from '../../app/api'
import { fetchNodeEdgeCounts } from './edgeCountsCache'
import useGraphViewState, { aggregateCounts, categoryKey, expandedKey } from './useGraphViewState'
import { parseGraphViewConfig } from './graphViewConfig'

const mockedFetch = vi.mocked(fetchNodeEdgeCounts)
const mockedPost = vi.mocked(post)

const node = (id: string, name: string) => new GraphNodeRef({ 'grebi:nodeId': id, 'grebi:name': [name] })
const root = node('root', 'Root')
const child = node('child', 'Child')
const grandchild = node('grandchild', 'Grandchild')

const rootCounts = {
  incoming: { is_a: { ols: 5 }, mentioned_by: { pubmed: 1 } },
  outgoing: { has_part: { ols: 2, reactome: 3 } },
}
const childCounts = { incoming: {}, outgoing: { regulates: { newds: 7 } } }
const empty = { incoming: {}, outgoing: {} }

function countsFor(enc: string) {
  if (enc === child.getEncodedNodeId()) return childCounts
  if (enc === root.getEncodedNodeId()) return rootCounts
  return empty
}

beforeEach(() => {
  mockedFetch.mockReset()
  mockedPost.mockReset()
  mockedPost.mockResolvedValue({})
  mockedFetch.mockImplementation(async (_graph, enc) => countsFor(enc))
})

async function loadRoot() {
  const hook = renderHook(() => useGraphViewState('g'))
  await act(async () => {
    await hook.result.current.loadEdgeCounts(root)
  })
  return hook
}

const keysOf = (m: Map<string, any>) => Array.from(m.keys()).sort()

describe('aggregateCounts', () => {
  it('sums per edge type, drops excluded datasources and hidden types, and sorts by total', () => {
    const res = aggregateCounts(
      { a: { d1: 1, d2: 4 }, b: { d1: 10 }, c: { d2: 3 }, hidden: { d1: 99 } },
      new Set(['d2']),
      new Set(['hidden'])
    )
    expect(res.map((r) => [r.edgeType, r.totalCount])).toEqual([['b', 10], ['a', 1]])
    // datasources and dsToCount are reported unfiltered so the UI can still show them
    expect(res[1]).toMatchObject({ datasources: ['d1', 'd2'], dsToCount: { d1: 1, d2: 4 } })
  })
})

describe('useGraphViewState', () => {
  it('loads the root\'s counts, datasources and sorted aggregates', async () => {
    const { result } = await loadRoot()
    expect(result.current.root?.getNodeId()).toBe('root')
    expect(result.current.loading).toBe(false)
    expect(result.current.error).toBeNull()
    expect(mockedFetch).toHaveBeenCalledWith('g', root.getEncodedNodeId())
    expect(result.current.allDatasources).toEqual(['ols', 'pubmed', 'reactome'])
    expect(result.current.incomingAggregated.map((a) => [a.edgeType, a.totalCount])).toEqual([['is_a', 5], ['mentioned_by', 1]])
    expect(result.current.outgoingAggregated).toEqual([
      { edgeType: 'has_part', datasources: ['ols', 'reactome'], totalCount: 5, dsToCount: { ols: 2, reactome: 3 } },
    ])
  })

  it('resolves count=1 edges to their node via resolve_single_edges', async () => {
    mockedPost.mockResolvedValue({ 'incoming::mentioned_by': { 'grebi:nodeId': 'pub1', 'grebi:name': ['Paper'] } })
    const { result } = await loadRoot()

    expect(mockedPost).toHaveBeenCalledWith(
      `api/v1/graphs/g/nodes/${root.getEncodedNodeId()}/resolve_single_edges`,
      {},
      [{ direction: 'incoming', edgeType: 'mentioned_by' }]
    )
    expect(result.current.autoExpandedNodes.get(expandedKey('root', 'incoming', 'mentioned_by'))?.getName()).toBe('Paper')

    // the resolve effect must not request the root's count=1 edges again once
    // loadEdgeCounts has auto-expanded them
    await act(async () => {})
    expect(mockedPost).toHaveBeenCalledTimes(1)
  })

  it('expands an edge: the node is marked loading until its counts arrive, then its datasources are merged', async () => {
    const { result } = await loadRoot()
    let resolveChild: (v: any) => void = () => {}
    mockedFetch.mockImplementation((_graph, enc) =>
      enc === child.getEncodedNodeId() ? new Promise((r) => { resolveChild = r }) : Promise.resolve(countsFor(enc))
    )

    let expandPromise: Promise<void>
    act(() => {
      expandPromise = result.current.expandEdge('root', 'outgoing', 'has_part', child)
    })
    const key = expandedKey('root', 'outgoing', 'has_part')
    expect(result.current.expandedNodes.get(key)).toMatchObject({ loading: true, parentNodeId: 'root', direction: 'outgoing', edgeType: 'has_part' })
    expect(result.current.anyExpansionLoading).toBe(true)

    await act(async () => {
      resolveChild(childCounts)
      await expandPromise
    })
    expect(result.current.expandedNodes.get(key)).toMatchObject({ loading: false, outgoingEdgeCounts: childCounts.outgoing })
    expect(result.current.anyExpansionLoading).toBe(false)
    expect(result.current.allDatasources).toEqual(['newds', 'ols', 'pubmed', 'reactome'])
  })

  it('keeps at most one expansion per parent, dropping the old branch and its descendants', async () => {
    const { result } = await loadRoot()
    await act(async () => { await result.current.expandEdge('root', 'outgoing', 'has_part', child) })
    await act(async () => { await result.current.expandEdge('child', 'outgoing', 'regulates', grandchild) })
    expect(keysOf(result.current.expandedNodes)).toEqual(['child::outgoing::regulates', 'root::outgoing::has_part'])

    const child2 = node('child2', 'Child Two')
    await act(async () => { await result.current.expandEdge('root', 'incoming', 'is_a', child2) })
    expect(keysOf(result.current.expandedNodes)).toEqual(['root::incoming::is_a'])
  })

  it('collapses an edge with its descendants, or only the descendants', async () => {
    const { result } = await loadRoot()
    await act(async () => { await result.current.expandEdge('root', 'outgoing', 'has_part', child) })
    await act(async () => { await result.current.expandEdge('child', 'outgoing', 'regulates', grandchild) })

    act(() => result.current.collapseDescendants('child'))
    expect(keysOf(result.current.expandedNodes)).toEqual(['root::outgoing::has_part'])

    await act(async () => { await result.current.expandEdge('child', 'outgoing', 'regulates', grandchild) })
    act(() => result.current.collapseEdge('root', 'outgoing', 'has_part'))
    expect(keysOf(result.current.expandedNodes)).toEqual([])
  })

  it('filters aggregates by datasource and edge type', async () => {
    const { result } = await loadRoot()

    act(() => result.current.toggleDsExclude(['ols', 'pubmed']))
    expect(Array.from(result.current.dsExclude)).toEqual(['reactome'])
    expect(result.current.outgoingAggregated[0].totalCount).toBe(2)

    act(() => result.current.toggleEdgeTypeHidden('is_a'))
    expect(result.current.incomingAggregated.map((a) => a.edgeType)).toEqual(['mentioned_by'])
    act(() => result.current.toggleEdgeTypeHidden('is_a'))
    expect(result.current.incomingAggregated.map((a) => a.edgeType)).toEqual(['is_a', 'mentioned_by'])

    act(() => result.current.hideAllEdgeTypes())
    expect(result.current.incomingAggregated).toEqual([])
    expect(result.current.outgoingAggregated).toEqual([])
    expect(Array.from(result.current.hiddenEdgeTypes).sort()).toEqual(['has_part', 'is_a', 'mentioned_by'])

    act(() => result.current.showAllEdgeTypes())
    expect(result.current.hiddenEdgeTypes.size).toBe(0)
  })

  it('keeps the same aggregates across renders until the counts or filters change', async () => {
    const { result, rerender } = await loadRoot()
    const { incomingAggregated, outgoingAggregated } = result.current
    rerender()
    expect(result.current.incomingAggregated).toBe(incomingAggregated)
    expect(result.current.outgoingAggregated).toBe(outgoingAggregated)

    act(() => result.current.toggleEdgeTypeHidden('is_a'))
    expect(result.current.incomingAggregated).not.toBe(incomingAggregated)
    expect(result.current.incomingAggregated.map((a) => a.edgeType)).toEqual(['mentioned_by'])
  })

  it('reports an error when the counts fail to load', async () => {
    mockedFetch.mockRejectedValue(new Error('boom'))
    const { result } = renderHook(() => useGraphViewState('g'))
    await act(async () => { await result.current.loadEdgeCounts(root) })
    expect(result.current.error).toBe('boom')
    expect(result.current.loading).toBe(false)
  })

  it('ignores the result of a superseded load', async () => {
    let resolveFirst: (v: any) => void = () => {}
    const other = node('other', 'Other')
    mockedFetch
      .mockImplementationOnce(() => new Promise((r) => { resolveFirst = r }))
      .mockResolvedValueOnce({ incoming: { second: { ds: 9 } }, outgoing: {} })

    const { result } = renderHook(() => useGraphViewState('g'))
    let firstLoad: Promise<void>
    act(() => { firstLoad = result.current.loadEdgeCounts(root) })
    await act(async () => { await result.current.loadEdgeCounts(other) })
    expect(result.current.root?.getNodeId()).toBe('other')
    expect(result.current.loading).toBe(false)

    await act(async () => {
      resolveFirst(rootCounts)
      await firstLoad
    })
    expect(result.current.root?.getNodeId()).toBe('other')
    expect(result.current.incomingAggregated.map((a) => a.edgeType)).toEqual(['second'])
  })
})

describe('useGraphViewState with a graph view config', () => {
  // rootCounts has is_a and mentioned_by incoming, has_part outgoing; childCounts has regulates
  const config = parseGraphViewConfig({
    max_stems: 12,
    hidden_edge_types: ['mentioned_by', 'regulates'],
    categories: [{ id: 'meta', label: 'Meta', hidden: true, edge_types: ['has_part'] }],
  })
  async function load() {
    const hook = renderHook(() => useGraphViewState('g', config))
    await act(async () => {
      await hook.result.current.loadEdgeCounts(root)
    })
    return hook
  }

  it('hides what the config hides at first, and keeps only the user\'s changes to that', async () => {
    const { result } = await load()
    expect(Array.from(result.current.hiddenEdgeTypes).sort()).toEqual(['has_part', 'mentioned_by'])
    expect(result.current.edgeTypeOverrides.size).toBe(0)
    expect(result.current.incomingAggregated.map((a) => a.edgeType)).toEqual(['is_a'])
    expect(result.current.outgoingAggregated).toEqual([])

    act(() => result.current.toggleEdgeTypeHidden('has_part'))
    expect(result.current.hiddenEdgeTypes.has('has_part')).toBe(false)
    expect(Array.from(result.current.edgeTypeOverrides)).toEqual([['has_part', true]])
    act(() => result.current.toggleEdgeTypeHidden('is_a'))
    expect(result.current.edgeTypeOverrides.get('is_a')).toBe(false)
    // hidden again, as it is at first: no longer a change
    act(() => result.current.toggleEdgeTypeHidden('has_part'))
    expect(result.current.edgeTypeOverrides.has('has_part')).toBe(false)

    act(() => result.current.showAllEdgeTypes())
    expect(result.current.hiddenEdgeTypes.size).toBe(0)
    act(() => result.current.hideAllEdgeTypes())
    expect(Array.from(result.current.hiddenEdgeTypes).sort()).toEqual(['has_part', 'is_a', 'mentioned_by'])
    expect(Array.from(result.current.edgeTypeOverrides)).toEqual([['is_a', false]])

    act(() => result.current.setEdgeTypesHidden(['has_part', 'mentioned_by'], false))
    expect(Array.from(result.current.hiddenEdgeTypes)).toEqual(['is_a'])
  })

  it('restores filters from the URL as changes to the defaults', async () => {
    const { result } = await load()
    act(() => result.current.setFilters(['pubmed'], ['is_a', 'has_part'], ['has_part', 'mentioned_by', 'x:shown_anyway']))
    expect(Array.from(result.current.dsExclude)).toEqual(['pubmed'])
    // has_part is both: shown wins, as it is applied second
    expect(Object.fromEntries(result.current.edgeTypeOverrides)).toEqual({ is_a: false, has_part: true, mentioned_by: true })
    expect(Array.from(result.current.hiddenEdgeTypes)).toEqual(['is_a'])
  })

  it('hides what the config hides at expanded nodes too', async () => {
    const { result } = await load()
    await act(async () => {
      await result.current.expandEdge('root', 'outgoing', 'has_part', child)
    })
    expect(result.current.hiddenEdgeTypes.has('regulates')).toBe(true)
  })

  it('remembers how far each category is opened, until another node is loaded', async () => {
    const { result } = await load()
    const key = categoryKey('root', 'outgoing', 'assoc')
    act(() => result.current.setCategoryOpening('root', 'outgoing', 'assoc', 'open'))
    expect(result.current.openCategories.get(key)).toBe('open')
    act(() => result.current.setCategoryOpening('root', 'outgoing', 'assoc', 'all'))
    expect(result.current.openCategories.get(key)).toBe('all')
    act(() => result.current.setCategoryOpening('root', 'outgoing', 'assoc', null))
    expect(result.current.openCategories.size).toBe(0)

    act(() => result.current.setCategoryOpening('root', 'outgoing', 'assoc', 'open'))
    act(() => result.current.toggleEdgeTypeHidden('is_a'))
    await act(async () => {
      await result.current.loadEdgeCounts(child)
    })
    expect(result.current.openCategories.size).toBe(0)
    expect(result.current.edgeTypeOverrides.size).toBe(0)
  })
})
