import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render } from '@testing-library/react'
import GraphRenderer from './GraphRenderer'
import { GraphLayout } from './graphLayout'

// sigma needs WebGL, which jsdom does not have: the module even throws at import
// time. Replace it with a fake that records the graph, settings and handlers.
const { sigmaInstances } = vi.hoisted(() => ({ sigmaInstances: [] as any[] }))

vi.mock('sigma', () => {
  class FakeSigma {
    graph: any
    container: HTMLElement
    settings: Record<string, any>
    handlers: Record<string, Function[]> = {}
    camera = { animate: vi.fn() }
    refresh = vi.fn()
    kill = vi.fn()
    constructor(graph: any, container: HTMLElement, settings: Record<string, any>) {
      this.graph = graph
      this.container = container
      this.settings = { ...settings }
      sigmaInstances.push(this)
    }
    on(event: string, fn: Function) {
      ;(this.handlers[event] ||= []).push(fn)
    }
    emit(event: string, payload: any) {
      for (const fn of this.handlers[event] || []) fn(payload)
    }
    getGraph() { return this.graph }
    setGraph(graph: any) { this.graph = graph }
    setSetting(key: string, value: any) { this.settings[key] = value }
    getCamera() { return this.camera }
  }
  return { default: FakeSigma }
})

vi.mock('sigma/rendering', () => ({
  createEdgeArrowProgram: () => class {},
  NodeProgram: class {},
  DEFAULT_EDGE_ARROW_HEAD_PROGRAM_OPTIONS: {},
}))

vi.mock('@sigma/edge-curve', () => ({
  createEdgeCurveProgram: () => class {},
  createDrawCurvedEdgeLabel: () => () => {},
  // stand-in for the real indexer: number edges that share both endpoints
  indexParallelEdgesIndex: (graph: any) => {
    const groups = new Map<string, string[]>()
    graph.forEachEdge((edge: string, _attrs: any, source: string, target: string) => {
      const key = [source, target].sort().join('|')
      if (!groups.has(key)) groups.set(key, [])
      groups.get(key)!.push(edge)
    })
    for (const edges of groups.values()) {
      if (edges.length < 2) continue
      edges.forEach((e, i) => {
        graph.setEdgeAttribute(e, 'parallelIndex', i)
        graph.setEdgeAttribute(e, 'parallelMaxIndex', edges.length - 1)
      })
    }
  },
}))

const layout: GraphLayout = {
  nodes: [
    { id: 'root', x: 0, y: 0, size: 40, label: 'Root', type: 'root', color: '#555', labelColor: '#fff' },
    {
      id: 'count::root::outgoing::has_part', x: 250, y: 0, size: 30, label: '5', type: 'count', color: '#eee', labelColor: '#666',
      direction: 'outgoing', edgeType: 'has_part', count: 5, datasources: ['ds1'], dsToCount: { ds1: 5 },
      parentNodeId: 'root', parentEncodedNodeId: 'cm9vdA',
    },
    {
      id: 'child', x: -250, y: 0, size: 28, label: 'Child', type: 'expanded_node', color: '#4a90d9', labelColor: '#fff',
      direction: 'incoming', edgeType: 'is_a', datasources: ['ds2'], parentNodeId: 'root', parentEncodedNodeId: 'cm9vdA',
    },
    {
      id: 'auto', x: 100, y: 100, size: 28, label: 'Auto', type: 'auto_expanded_node', color: '#4a90d9', labelColor: '#fff',
      direction: 'outgoing', edgeType: 'part_of', parentNodeId: 'root', parentEncodedNodeId: 'cm9vdA',
    },
  ],
  edges: [
    { id: 'e1', source: 'root', target: 'count::root::outgoing::has_part', label: 'has_part', color: '#ccc', type: 'stem', dashed: true, size: 1.5, direction: 'outgoing', edgeType: 'has_part', datasources: ['ds1'] },
    { id: 'e2', source: 'child', target: 'root', label: 'is_a', color: '#4a90d9', type: 'expanded', dashed: false, size: 2, direction: 'incoming', edgeType: 'is_a', datasources: ['ds2'] },
    { id: 'e3', source: 'root', target: 'auto', label: 'part_of', color: '#4a90d9', type: 'expanded', dashed: false, size: 2, direction: 'outgoing', edgeType: 'part_of', datasources: ['ds1'] },
    { id: 'e4', source: 'root', target: 'auto', label: 'related_to', color: '#4a90d9', type: 'expanded', dashed: false, size: 2, direction: 'outgoing', edgeType: 'related_to', datasources: ['ds1'] },
    // dangling edge: its target is not in the layout
    { id: 'e5', source: 'root', target: 'missing', label: 'x', color: '#ccc', type: 'stem', dashed: true, size: 1, direction: 'outgoing', edgeType: 'x' },
  ],
}

function renderGraph(overrides: Partial<React.ComponentProps<typeof GraphRenderer>> = {}) {
  const props = {
    layout,
    onClickRoot: vi.fn(),
    onClickCountNode: vi.fn(),
    onClickExpandedNode: vi.fn(),
    onClickAutoExpandedNode: vi.fn(),
    onDoubleClickExpandedNode: vi.fn(),
    highlightedDatasource: null,
    highlightedEdgeType: null,
    focusNodeIds: null,
    onHoverNode: vi.fn(),
    onLeaveNode: vi.fn(),
    ...overrides,
  }
  const utils = render(<GraphRenderer {...props} />)
  return { props, sigma: sigmaInstances[sigmaInstances.length - 1], ...utils }
}

const edgeBetween = (graph: any, source: string, target: string, label: string) =>
  graph.edges().find((e: string) => graph.source(e) === source && graph.target(e) === target && graph.getEdgeAttribute(e, 'label') === label)

beforeEach(() => {
  sigmaInstances.length = 0
})

describe('GraphRenderer', () => {
  it('builds a graphology graph from the layout and hands it to sigma', () => {
    const { sigma } = renderGraph()
    expect(sigmaInstances).toHaveLength(1)
    const graph = sigma.graph
    expect(graph.nodes().sort()).toEqual(['auto', 'child', 'count::root::outgoing::has_part', 'root'])
    expect(graph.getNodeAttributes('count::root::outgoing::has_part')).toMatchObject({
      nodeType: 'count', label: '5', direction: 'outgoing', edgeType: 'has_part', count: 5,
      parentNodeId: 'root', parentEncodedNodeId: 'cm9vdA', forceLabel: true, hasHiddenChildren: false,
    })
    // the dangling edge is dropped; the rest are curved arrows
    expect(graph.size).toBe(4)
    graph.forEachEdge((_e: string, attrs: any) => expect(attrs.type).toBe('curvedArrow'))
    // single edges get a gentle curve, parallel edges fan out
    expect(graph.getEdgeAttribute(edgeBetween(graph, 'root', 'count::root::outgoing::has_part', 'has_part'), 'curvature')).toBe(0.15)
    expect(graph.getEdgeAttribute(edgeBetween(graph, 'root', 'auto', 'part_of'), 'curvature')).toBe(0)
    expect(graph.getEdgeAttribute(edgeBetween(graph, 'root', 'auto', 'related_to'), 'curvature')).toBe(0.5)
    expect(sigma.settings.defaultEdgeType).toBe('curvedArrow')
  })

  it('routes node clicks to the callback for the node type', () => {
    const { props, sigma } = renderGraph()
    sigma.emit('clickNode', { node: 'root' })
    expect(props.onClickRoot).toHaveBeenCalledTimes(1)
    sigma.emit('clickNode', { node: 'count::root::outgoing::has_part' })
    expect(props.onClickCountNode).toHaveBeenCalledWith('root', 'cm9vdA', 'outgoing', 'has_part')
    sigma.emit('clickNode', { node: 'child' })
    expect(props.onClickExpandedNode).toHaveBeenCalledWith('root', 'cm9vdA', 'incoming', 'is_a')
    sigma.emit('clickNode', { node: 'auto' })
    expect(props.onClickAutoExpandedNode).toHaveBeenCalledWith('root', 'cm9vdA', 'outgoing', 'part_of')
  })

  it('handles double clicks on expanded and auto-expanded nodes only', () => {
    const { props, sigma } = renderGraph()
    const event = { preventSigmaDefault: vi.fn() }
    sigma.emit('doubleClickNode', { node: 'child', event })
    expect(props.onDoubleClickExpandedNode).toHaveBeenCalledWith('root', 'incoming', 'is_a', 'child')
    sigma.emit('doubleClickNode', { node: 'auto', event })
    expect(props.onDoubleClickExpandedNode).toHaveBeenCalledWith('root', 'outgoing', 'part_of', 'auto')
    sigma.emit('doubleClickNode', { node: 'count::root::outgoing::has_part', event })
    sigma.emit('doubleClickNode', { node: 'root', event })
    expect(props.onDoubleClickExpandedNode).toHaveBeenCalledTimes(2)
    expect(event.preventSigmaDefault).toHaveBeenCalledTimes(4)
  })

  it('reports hovered nodes (but not the root) and sets the pointer cursor', () => {
    const { props, sigma } = renderGraph()
    sigma.emit('enterNode', { node: 'child' })
    expect(props.onHoverNode).toHaveBeenCalledWith(layout.nodes[2])
    expect(sigma.container.style.cursor).toBe('pointer')
    sigma.emit('leaveNode', {})
    expect(props.onLeaveNode).toHaveBeenCalledTimes(1)
    expect(sigma.container.style.cursor).toBe('default')

    sigma.emit('enterNode', { node: 'root' })
    expect(props.onHoverNode).toHaveBeenCalledTimes(1)
    expect(sigma.container.style.cursor).toBe('pointer')
  })

  it('reuses one sigma instance across layout changes and kills it on unmount', () => {
    const { props, sigma, rerender, unmount } = renderGraph()
    expect(sigma.camera.animate).toHaveBeenCalledTimes(1)

    const smaller: GraphLayout = { nodes: [layout.nodes[0]], edges: [] }
    rerender(<GraphRenderer {...props} layout={smaller} />)
    expect(sigmaInstances).toHaveLength(1)
    expect(sigma.graph.nodes()).toEqual(['root'])
    expect(sigma.refresh).toHaveBeenCalled()
    expect(sigma.camera.animate).toHaveBeenCalledTimes(2)

    unmount()
    expect(sigma.kill).toHaveBeenCalledTimes(1)
  })

  it('installs reducers that fade nodes and edges outside the highlighted datasource or edge type', () => {
    const { props, sigma, rerender } = renderGraph()
    // nothing highlighted: data passes through untouched
    const data = { color: '#eee', label: '5', datasources: ['ds1'], dsToCount: { ds1: 5 }, nodeType: 'count', edgeType: 'has_part', size: 1.5 }
    expect(sigma.settings.nodeReducer('n', data)).toBe(data)

    rerender(<GraphRenderer {...props} highlightedDatasource="ds1" />)
    const nodeReducer = sigma.settings.nodeReducer
    expect(nodeReducer('n', data)).toMatchObject({ color: '#7323b7', labelColor: '#ffffff', label: '5' })
    expect(nodeReducer('n', { ...data, datasources: ['ds2'] })).toMatchObject({ color: 'rgba(0,0,0,0)', label: '' })
    expect(nodeReducer('n', { ...data, datasources: ['ds2'], nodeType: 'root' }).color).toBe('#eee')
    // ontology datasources get the ontology colour
    rerender(<GraphRenderer {...props} highlightedDatasource="OLS.mondo" />)
    expect(sigma.settings.nodeReducer('n', { ...data, datasources: ['OLS.mondo'] }).color).toBe('#00827c')

    rerender(<GraphRenderer {...props} highlightedDatasource={null} highlightedEdgeType="is_a" />)
    const edgeReducer = sigma.settings.edgeReducer
    expect(edgeReducer('e', { ...data, edgeType: 'is_a' })).toMatchObject({ color: '#2196f3', size: 2, _faded: false })
    expect(edgeReducer('e', data)).toMatchObject({ color: '#e0e0e0', size: 0.5, _faded: true })
    expect(sigma.refresh).toHaveBeenCalled()
  })
})
