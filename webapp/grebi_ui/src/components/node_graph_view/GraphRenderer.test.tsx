import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import GraphRenderer from './GraphRenderer'
import { GraphLayout } from './graphLayout'

// sigma needs WebGL, which jsdom does not have: the module even throws at import
// time. Replace it with a fake that records the graph, settings and handlers.
const { sigmaInstances, sigmaFails, edgeLabelsDrawn } = vi.hoisted(() => ({ sigmaInstances: [] as any[], sigmaFails: { value: false }, edgeLabelsDrawn: [] as any[][] }))

vi.mock('sigma', () => {
  class FakeSigma {
    graph: any
    container: HTMLElement
    settings: Record<string, any>
    handlers: Record<string, Function[]> = {}
    camera = { animate: vi.fn() }
    // sigma reads the custom bounding box when it processes the graph (on
    // construction, refresh and setGraph): record the box each of those saw
    customBBox: any = null
    processedBBox: any = undefined
    refresh = vi.fn(() => { this.processedBBox = this.customBBox })
    kill = vi.fn()
    constructor(graph: any, container: HTMLElement, settings: Record<string, any>) {
      if (sigmaFails.value) throw new Error('WebGL context could not be created')
      this.graph = graph
      this.container = container
      this.settings = { ...settings }
      this.processedBBox = this.customBBox
      sigmaInstances.push(this)
    }
    on(event: string, fn: Function) {
      ;(this.handlers[event] ||= []).push(fn)
    }
    emit(event: string, payload: any) {
      for (const fn of this.handlers[event] || []) fn(payload)
    }
    getGraph() { return this.graph }
    setGraph(graph: any) { this.graph = graph; this.processedBBox = this.customBBox }
    setCustomBBox(bbox: any) { this.customBBox = bbox }
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
  // stand-in for the label drawer: records what it was asked to draw
  createDrawCurvedEdgeLabel: () => (...args: any[]) => { edgeLabelsDrawn.push(args) },
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

// a layout spanning more than the ring of stems (500 units) both ways
const large: GraphLayout = {
  nodes: [layout.nodes[0], { ...layout.nodes[2], id: 'a', x: -300, y: -300 }, { ...layout.nodes[2], id: 'b', x: 300, y: 300 }],
  edges: [],
}

const edgeBetween = (graph: any, source: string, target: string, label: string) =>
  graph.edges().find((e: string) => graph.source(e) === source && graph.target(e) === target && graph.getEdgeAttribute(e, 'label') === label)

// jsdom has no canvas contexts; the renderer checks for WebGL before starting sigma
let getContext: any
beforeEach(() => {
  sigmaInstances.length = 0
  edgeLabelsDrawn.length = 0
  sigmaFails.value = false
  getContext = vi.spyOn(HTMLCanvasElement.prototype, 'getContext').mockImplementation(() => ({}) as any)
})
afterEach(() => {
  getContext.mockRestore()
})

describe('GraphRenderer without WebGL', () => {
  it('says the graph view is not available instead of starting sigma', () => {
    getContext.mockImplementation(() => null)
    renderGraph()
    expect(screen.getByRole('note')).toHaveTextContent('The graph view is not available: this browser does not provide WebGL.')
    expect(sigmaInstances).toHaveLength(0)
  })

  it('says so too when sigma cannot start', () => {
    sigmaFails.value = true
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    renderGraph()
    expect(screen.getByRole('note')).toBeInTheDocument()
    expect(warn).toHaveBeenCalledWith('The graph view could not start', expect.any(Error))
    warn.mockRestore()
  })
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
    expect(props.onDoubleClickExpandedNode).toHaveBeenCalledWith('root', 'incoming', 'is_a', 'child', { newTab: false })
    sigma.emit('doubleClickNode', { node: 'auto', event })
    expect(props.onDoubleClickExpandedNode).toHaveBeenCalledWith('root', 'outgoing', 'part_of', 'auto', { newTab: false })
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

  it('fits a layout smaller than the ring of stems at the ring\'s size, and a larger one as it is', () => {
    // the root alone (every edge type hidden) has no extent of its own
    const rootOnly: GraphLayout = { nodes: [layout.nodes[0]], edges: [] }
    const { props, sigma, rerender } = renderGraph({ layout: rootOnly })
    expect(sigma.processedBBox).toEqual({ x: [-250, 250], y: [-250, 250] })

    rerender(<GraphRenderer {...props} layout={large} />)
    expect(sigma.processedBBox).toBeNull()

    rerender(<GraphRenderer {...props} layout={rootOnly} />)
    expect(sigma.processedBBox).toEqual({ x: [-250, 250], y: [-250, 250] })
    expect(sigmaInstances).toHaveLength(1)
  })

  it('leaves sigma to fit a layout larger than the ring from the start', () => {
    const { sigma } = renderGraph({ layout: large })
    expect(sigma.processedBBox).toBeNull()
    expect(sigma.customBBox).toBeNull()
  })

  it('reports clicks on category bubbles, and highlights them for any of their edge types', () => {
    const category = {
      id: 'category::root::outgoing::assoc', x: 250, y: 0, size: 30, label: '9', type: 'category' as const, color: '#ede7f6', labelColor: '#4527a0',
      direction: 'outgoing' as const, count: 9, edgeTypes: ['a', 'b'], categoryId: 'assoc', categoryLabel: 'Associations', open: false,
      parentNodeId: 'root', parentEncodedNodeId: 'cm9vdA',
    }
    const categoryLayout: GraphLayout = {
      nodes: [layout.nodes[0], category],
      edges: [{ id: 'e', source: 'root', target: category.id, label: 'Associations \u00b7 2 types', color: '#b39ddb', type: 'stem', dashed: true, size: 1.5, direction: 'outgoing', edgeTypes: ['a', 'b'] }],
    }
    const onClickCategory = vi.fn()
    const { props, sigma, rerender } = renderGraph({ layout: categoryLayout, onClickCategory })
    expect(sigma.graph.getNodeAttributes(category.id)).toMatchObject({ nodeType: 'category', categoryId: 'assoc', edgeTypes: ['a', 'b'] })

    sigma.emit('clickNode', { node: category.id })
    expect(onClickCategory).toHaveBeenCalledWith(category)
    sigma.emit('enterNode', { node: category.id })
    expect(sigma.container.style.cursor).toBe('pointer')

    rerender(<GraphRenderer {...props} highlightedEdgeType="b" />)
    expect(sigma.settings.nodeReducer(category.id, sigma.graph.getNodeAttributes(category.id))).toMatchObject({ color: '#2196f3', labelColor: '#333333' })
    const edge = sigma.graph.edges()[0]
    expect(sigma.settings.edgeReducer(edge, sigma.graph.getEdgeAttributes(edge))).toMatchObject({ color: '#2196f3', _faded: false })
  })

  it('draws the label of a category\'s stem in bold, a single edge type\'s as it is, and none on a faded edge', () => {
    const { sigma } = renderGraph()
    const draw = sigma.settings.defaultDrawEdgeLabel
    const settings = { edgeLabelWeight: 'normal', edgeLabelSize: 11 }

    draw({}, { label: 'Associations \u00b7 7 types', edgeTypes: ['a', 'b'] }, {}, {}, settings)
    expect(edgeLabelsDrawn).toHaveLength(1)
    expect(edgeLabelsDrawn[0][4]).toEqual({ edgeLabelWeight: 'bold', edgeLabelSize: 11 })

    draw({}, { label: 'biolink:affects', edgeType: 'biolink:affects' }, {}, {}, settings)
    expect(edgeLabelsDrawn[1][4]).toBe(settings)

    draw({}, { label: 'Associations \u00b7 7 types', edgeTypes: ['a', 'b'], _faded: true }, {}, {}, settings)
    expect(edgeLabelsDrawn).toHaveLength(2)
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
