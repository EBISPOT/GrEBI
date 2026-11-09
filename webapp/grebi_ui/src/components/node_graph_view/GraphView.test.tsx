import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, act, waitFor } from '@testing-library/react'
import GraphNode from '../../model/GraphNode'
import encodeNodeId from '../../encodeNodeId'
import GraphView from './GraphView'

// GraphRenderer wraps sigma (WebGL). Replace it with a stub that exposes the
// layout as buttons so the click/hover wiring in GraphView can be exercised.
const downloadImage = vi.hoisted(() => vi.fn())
// the layout the stub was given on each of its renders
const rendererLayouts = vi.hoisted(() => [] as any[])
vi.mock('./GraphRenderer', async () => {
  const React = await import('react')
  return {
    default: React.forwardRef((props: any, ref: any) => {
      React.useImperativeHandle(ref, () => ({ downloadImage }))
      rendererLayouts.push(props.layout)
      return (
        <div data-testid="renderer" data-ds={props.highlightedDatasource ?? ''} data-et={props.highlightedEdgeType ?? ''}>
          {props.layout.nodes.map((n: any) => (
            <button
              key={n.id}
              onClick={() => {
                if (n.type === 'root') props.onClickRoot()
                else if (n.type === 'count') props.onClickCountNode(n.parentNodeId, n.parentEncodedNodeId, n.direction, n.edgeType)
                else if (n.type === 'expanded_node') props.onClickExpandedNode(n.parentNodeId, n.parentEncodedNodeId, n.direction, n.edgeType)
                else if (n.type === 'category' || n.type === 'more') props.onClickCategory(n)
                else props.onClickAutoExpandedNode(n.parentNodeId, n.parentEncodedNodeId, n.direction, n.edgeType)
              }}
              onDoubleClick={(e) => props.onDoubleClickExpandedNode(n.parentNodeId, n.direction, n.edgeType, n.id, { newTab: e.ctrlKey || e.metaKey })}
              onContextMenu={(e) => { e.preventDefault(); props.onContextMenuNode?.(n, 12, 34) }}
              onMouseEnter={() => props.onHoverNode?.(n)}
              onMouseLeave={() => props.onLeaveNode?.()}
            >
              {n.id}
            </button>
          ))}
        </div>
      )
    }),
  }
})

vi.mock('./EdgeExpandPanel', async () => {
  const { default: GraphNodeRef } = await import('../../model/GraphNodeRef')
  return {
    default: (props: any) => (
      <div data-testid="expand-panel">
        {`panel:${props.nodeId}:${props.direction}:${props.edgeType}:chain=${props.chain.map((c: any) => c.label).join('>')}`}
        <button onClick={() => props.onSelectNode(new GraphNodeRef({ 'grebi:nodeId': 'child', 'grebi:name': ['Child'] }))}>pick</button>
      </div>
    ),
  }
})

vi.mock('../../app/api', () => ({ post: vi.fn(async () => ({})), get: vi.fn(), getPaginated: vi.fn() }))
vi.mock('./edgeCountsCache', () => ({ fetchNodeEdgeCounts: vi.fn(), prefetchNodeEdgeCounts: vi.fn() }))

import { fetchNodeEdgeCounts } from './edgeCountsCache'
import { get, post } from '../../app/api'
import { parseExploration, serialiseExploration } from './exploration'
const mockedFetch = vi.mocked(fetchNodeEdgeCounts)
const mockedGet = vi.mocked(get)
const mockedPost = vi.mocked(post)

const rootNode = new GraphNode({ 'grebi:nodeId': 'root', 'grebi:name': ['Root'] })
const counts: Record<string, any> = {
  root: { incoming: { is_a: { 'OLS.mondo': 5 } }, outgoing: { has_part: { ds1: 3 } } },
  child: { incoming: {}, outgoing: { regulates: { ds1: 2 } } },
  // two edge types the shipped config hides at first
  meta: { incoming: {}, outgoing: { has_part: { ds1: 3 }, 'rdf:type': { ds1: 2 }, 'ols_top_k:matches': { ds1: 40 } } },
  // more stems than the shipped config's max_stems (12), so they are gathered by category
  busy: {
    incoming: {},
    outgoing: Object.fromEntries([
      'biolink:subclass_of',
      'biolink:gene_to_disease_association', 'biolink:chemical_to_disease_or_phenotypic_feature_association',
      'biolink:affects', 'biolink:contributes_to', 'biolink:causes', 'biolink:treats',
      'biolink:has_part', 'biolink:part_of', 'biolink:overlaps',
      'biolink:orthologous_to', 'biolink:homologous_to',
      'x:one', 'x:two',
    ].map((t, i) => [t, { ds1: 100 - i }])),
  },
}

beforeEach(() => {
  rendererLayouts.length = 0
  mockedFetch.mockReset()
  mockedFetch.mockImplementation(async (_g, enc) => counts[atob(enc)] || { incoming: {}, outgoing: {} })
})

afterEach(() => {
  vi.useRealTimers()
})

async function renderLoaded(node = rootNode) {
  const utils = render(<GraphView graph="g" node={node} />)
  await screen.findByTestId('renderer')
  return utils
}

describe('GraphView', () => {
  it('shows a loading overlay, then the controls and the rendered graph', async () => {
    render(<GraphView graph="g" node={rootNode} />)
    expect(screen.getByText('Loading graph...')).toBeInTheDocument()

    await screen.findByTestId('renderer')
    expect(screen.queryByText('Loading graph...')).toBeNull()
    expect(screen.getByRole('button', { name: 'count::root::incoming::is_a' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'count::root::outgoing::has_part' })).toBeInTheDocument()
    expect(screen.getByTitle('mondo')).toBeInTheDocument()
    expect(screen.getByTitle('ds1')).toBeInTheDocument()
    expect(screen.getByText('Edge Types (2/2)')).toBeInTheDocument()
  })

  it('tells the user when the node has no edges', async () => {
    render(<GraphView graph="g" node={new GraphNode({ 'grebi:nodeId': 'lonely', 'grebi:name': ['Lonely'] })} />)
    expect(await screen.findByText('No edges found for this node')).toBeInTheDocument()
    expect(screen.queryByText('Datasources')).toBeNull()
  })

  it('shows the error state when the counts cannot be loaded', async () => {
    mockedFetch.mockRejectedValue(new Error('backend down'))
    render(<GraphView graph="g" node={rootNode} />)
    expect(await screen.findByText('Error loading graph data')).toBeInTheDocument()
    expect(screen.getByText('backend down')).toBeInTheDocument()
  })

  it('opens the inline expand panel for a count bubble, with the path bar, and closes it again', async () => {
    await renderLoaded()
    fireEvent.click(screen.getByRole('button', { name: 'count::root::outgoing::has_part' }))

    expect(screen.getByText('Choose a node to continue exploring')).toBeInTheDocument()
    expect(screen.getByTestId('expand-panel')).toHaveTextContent('panel:root:outgoing:has_part:chain=')
    // path bar reads: Root -> has_part -> 3 nodes (the arrows are icons)
    expect(screen.getByText('3 nodes').parentElement).toHaveTextContent(/^Root.*has_part.*3 nodes$/)
    expect(screen.queryByTestId('renderer')).toBeNull()

    fireEvent.click(screen.getByTestId('CloseIcon').closest('button')!)
    expect(screen.queryByTestId('expand-panel')).toBeNull()
    expect(screen.getByTestId('renderer')).toBeInTheDocument()
  })

  it('expands the picked node, shows its children, and can open the panel further down the chain', async () => {
    await renderLoaded()
    fireEvent.click(screen.getByRole('button', { name: 'count::root::outgoing::has_part' }))
    fireEvent.click(screen.getByRole('button', { name: 'pick' }))

    // the expanded node replaces the count bubble and brings its own children
    expect(await screen.findByRole('button', { name: 'child' })).toBeInTheDocument()
    expect(await screen.findByRole('button', { name: 'count::child::outgoing::regulates' })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'count::root::outgoing::has_part' })).toBeNull()
    // while root has an expansion, its other edges are not shown
    expect(screen.queryByRole('button', { name: 'count::root::incoming::is_a' })).toBeNull()

    // hovering the expanded node names both ends of the edge
    fireEvent.mouseEnter(screen.getByRole('button', { name: 'child' }))
    expect(screen.getByText('Child').parentElement).toHaveTextContent(/^Root.*has_part.*Child$/)

    // a count bubble under the expanded node opens the panel with the chain so far
    fireEvent.click(screen.getByRole('button', { name: 'count::child::outgoing::regulates' }))
    expect(screen.getByTestId('expand-panel')).toHaveTextContent('panel:child:outgoing:regulates:chain=Child')
  })

  it('shows the hover path bar for count bubbles and hides it on leave', async () => {
    await renderLoaded()
    const bubble = screen.getByRole('button', { name: 'count::root::incoming::is_a' })
    fireEvent.mouseEnter(bubble)
    // incoming: the unknown nodes are on the left, the root on the right
    expect(screen.getByText('5 nodes').parentElement).toHaveTextContent(/^5 nodes.*is_a.*Root$/)
    fireEvent.mouseLeave(bubble)
    expect(screen.queryByText('5 nodes')).toBeNull()
  })

  it('forgets the hovered node when the expand panel takes the graph\'s place', async () => {
    await renderLoaded()
    const bubble = screen.getByRole('button', { name: 'count::root::incoming::is_a' })
    fireEvent.mouseEnter(bubble)
    expect(screen.getByText('5 nodes')).toBeInTheDocument()
    // the bubble is clicked while hovered, and the graph it was in goes: it never reports being left
    fireEvent.click(bubble)
    expect(screen.getByTestId('expand-panel')).toBeInTheDocument()
    fireEvent.click(screen.getByTestId('CloseIcon').closest('button')!)
    expect(screen.getByTestId('renderer')).toBeInTheDocument()
    expect(screen.queryByText('5 nodes')).toBeNull()
  })

  it('names every edge type that leads to a node when it is hovered', async () => {
    counts.twice = { incoming: {}, outgoing: { parent_of: { ds1: 1 }, broader_than: { ds1: 1 } } }
    const target = { 'grebi:nodeId': 'n1', 'grebi:name': ['Node One'] }
    mockedPost.mockImplementation(async () => ({ 'outgoing::parent_of': target, 'outgoing::broader_than': target }) as any)
    try {
      render(<GraphView graph="g" node={new GraphNode({ 'grebi:nodeId': 'twice', 'grebi:name': ['Twice'] })} />)
      const node = await screen.findByRole('button', { name: 'n1' })
      fireEvent.mouseEnter(node)
      expect(screen.getByText('Node One').parentElement).toHaveTextContent(/^Twice.*(parent_of, broader_than|broader_than, parent_of).*Node One$/)
    } finally {
      mockedPost.mockImplementation(async () => ({}) as any)
      delete counts.twice
    }
  })

  it('does not re-render the graph when a node is hovered', async () => {
    await renderLoaded()
    const renders = rendererLayouts.length
    const bubble = screen.getByRole('button', { name: 'count::root::incoming::is_a' })
    fireEvent.mouseEnter(bubble)
    expect(screen.getByText('5 nodes')).toBeInTheDocument()
    fireEvent.mouseLeave(bubble)
    expect(rendererLayouts).toHaveLength(renders)
  })

  it('hands the renderer the same layout when only the highlight changes', async () => {
    await renderLoaded()
    const layout = rendererLayouts[rendererLayouts.length - 1]
    vi.useFakeTimers()
    fireEvent.mouseEnter(screen.getByTitle('ds1'))
    act(() => { vi.advanceTimersByTime(30) })
    expect(screen.getByTestId('renderer').dataset.ds).toBe('ds1')
    // a new layout object would make the renderer rebuild the graph and reset the camera
    expect(rendererLayouts[rendererLayouts.length - 1]).toBe(layout)
  })

  it('debounces datasource and edge type highlighting from the controls', async () => {
    await renderLoaded()
    vi.useFakeTimers()

    fireEvent.mouseEnter(screen.getByTitle('ds1'))
    expect(screen.getByTestId('renderer').dataset.ds).toBe('')
    act(() => { vi.advanceTimersByTime(30) })
    expect(screen.getByTestId('renderer').dataset.ds).toBe('ds1')
    fireEvent.mouseLeave(screen.getByTitle('ds1'))
    act(() => { vi.advanceTimersByTime(30) })
    expect(screen.getByTestId('renderer').dataset.ds).toBe('')

    fireEvent.mouseEnter(screen.getByText('is_a'))
    act(() => { vi.advanceTimersByTime(30) })
    expect(screen.getByTestId('renderer').dataset.et).toBe('is_a')
  })

  it('re-roots the graph on double click and returns to the original node when the root is clicked', async () => {
    await renderLoaded()
    fireEvent.click(screen.getByRole('button', { name: 'count::root::outgoing::has_part' }))
    fireEvent.click(screen.getByRole('button', { name: 'pick' }))
    const child = await screen.findByRole('button', { name: 'child' })

    fireEvent.doubleClick(child)
    await waitFor(() => expect(screen.getByRole('button', { name: 'count::child::outgoing::regulates' })).toBeInTheDocument())
    expect(screen.queryByRole('button', { name: 'root' })).toBeNull()
    expect(mockedFetch).toHaveBeenLastCalledWith('g', encodeNodeId('child'))

    fireEvent.click(screen.getByRole('button', { name: 'child' }))
    await waitFor(() => expect(screen.getByRole('button', { name: 'root' })).toBeInTheDocument())
    expect(screen.getByRole('button', { name: 'count::root::outgoing::has_part' })).toBeInTheDocument()
  })

  it('restores the exploration the page hands it, then reports what changes', async () => {
    mockedGet.mockImplementation(async (path: string) => ({ 'grebi:nodeId': atob(path.split('/').pop()!.split('?')[0]), 'grebi:name': ['Child'] }))
    const onChange = vi.fn()
    const token = serialiseExploration({ steps: [{ direction: 'outgoing', edgeType: 'has_part', nodeId: 'child' }], excludedDatasources: [], hiddenEdgeTypes: [], shownEdgeTypes: [] })
    render(<GraphView graph="g" node={rootNode} exploration={token} onExplorationChange={onChange} />)

    // the chain is rebuilt from the URL: the child is fetched and expanded
    expect(await screen.findByRole('button', { name: 'child' })).toBeInTheDocument()
    expect(await screen.findByRole('button', { name: 'count::child::outgoing::regulates' })).toBeInTheDocument()
    // only the node's name is needed, so it is asked for abridged
    expect(mockedGet).toHaveBeenCalledWith(`api/v1/graphs/g/nodes/${encodeNodeId('child')}?maxValues=100`)
    // what was restored is what the URL said, so nothing is written back
    expect(onChange).not.toHaveBeenCalled()

    // expanding further reports a new token as a history entry
    fireEvent.click(screen.getByRole('button', { name: 'count::child::outgoing::regulates' }))
    fireEvent.click(screen.getByRole('button', { name: 'pick' }))
    await waitFor(() => expect(onChange).toHaveBeenCalledTimes(1))
    const [token2, options2] = onChange.mock.calls[0]
    expect(options2).toEqual({ replace: false })
    expect(token2).not.toBe(token)

    // a filter change only replaces the entry
    fireEvent.click(screen.getByRole('checkbox', { name: 'is_a' }))
    await waitFor(() => expect(onChange).toHaveBeenCalledTimes(2))
    expect(onChange.mock.calls[1][1]).toEqual({ replace: true })
    expect(onChange.mock.calls[1][0]).not.toBe(token2)

    // the link button copies the page address
    const writeText = vi.fn(async () => {})
    vi.stubGlobal('navigator', { ...navigator, clipboard: { writeText } })
    fireEvent.click(screen.getByTitle('Copy link to this view'))
    await waitFor(() => expect(screen.getByTitle('Link copied')).toBeInTheDocument())
    expect(writeText).toHaveBeenCalledWith(window.location.href)
    vi.unstubAllGlobals()
  })

  it('hides the edge types the config hides at first, and records showing one in the URL', async () => {
    const onChange = vi.fn()
    render(<GraphView graph="g" node={new GraphNode({ 'grebi:nodeId': 'meta', 'grebi:name': ['Meta'] })} onExplorationChange={onChange} />)
    await screen.findByTestId('renderer')
    expect(screen.getByRole('button', { name: 'count::meta::outgoing::has_part' })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'count::meta::outgoing::rdf:type' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'count::meta::outgoing::ols_top_k:matches' })).toBeNull()
    expect(screen.getByText('Edge Types (1/3)')).toBeInTheDocument()
    expect(screen.getByRole('checkbox', { name: 'rdf:type' })).not.toBeChecked()
    // the defaults are not a change, so there is nothing to put in the URL
    expect(onChange).not.toHaveBeenCalled()

    fireEvent.click(screen.getByRole('checkbox', { name: 'rdf:type' }))
    expect(await screen.findByRole('button', { name: 'count::meta::outgoing::rdf:type' })).toBeInTheDocument()
    await waitFor(() => expect(onChange).toHaveBeenCalledTimes(1))
    expect(parseExploration(onChange.mock.calls[0][0])).toMatchObject({ hiddenEdgeTypes: [], shownEdgeTypes: ['rdf:type'] })
  })

  it('gathers a busy node\'s stems into category bubbles that open and gather back', async () => {
    render(<GraphView graph="g" node={new GraphNode({ 'grebi:nodeId': 'busy', 'grebi:name': ['Busy'] })} />)
    await screen.findByTestId('renderer')
    const associations = screen.getByRole('button', { name: 'category::busy::outgoing::associations' })
    // pinned edge types keep their own stems
    expect(screen.getByRole('button', { name: 'count::busy::outgoing::biolink:subclass_of' })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'count::busy::outgoing::biolink:affects' })).toBeNull()

    fireEvent.mouseEnter(associations)
    expect(screen.getByText('Associations: 6 edge types')).toBeInTheDocument()
    expect(screen.getByText('click to show them')).toBeInTheDocument()
    fireEvent.mouseLeave(associations)

    fireEvent.click(associations)
    expect(screen.getByRole('button', { name: 'count::busy::outgoing::biolink:affects' })).toBeInTheDocument()
    fireEvent.mouseEnter(screen.getByRole('button', { name: 'category::busy::outgoing::associations' }))
    expect(screen.getByText('click to gather them')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'category::busy::outgoing::associations' }))
    expect(screen.queryByRole('button', { name: 'count::busy::outgoing::biolink:affects' })).toBeNull()
  })

  it('goes to the node the page asks for on double click instead of re-rooting', async () => {
    const onNavigate = vi.fn()
    render(<GraphView graph="g" node={rootNode} onNavigateToNode={onNavigate} />)
    await screen.findByTestId('renderer')
    fireEvent.click(screen.getByRole('button', { name: 'count::root::outgoing::has_part' }))
    fireEvent.click(screen.getByRole('button', { name: 'pick' }))
    fireEvent.doubleClick(await screen.findByRole('button', { name: 'child' }))
    expect(onNavigate).toHaveBeenCalledTimes(1)
    expect(onNavigate.mock.calls[0][0].getNodeId()).toBe('child')
    // still rooted at the original node
    expect(screen.getByRole('button', { name: 'root' })).toBeInTheDocument()
  })

  it('saves the drawing as an image named after the root', async () => {
    await renderLoaded()
    fireEvent.click(screen.getByTitle('Download as image'))
    expect(downloadImage).toHaveBeenCalledWith('Root-graph')
  })

  it('offers a menu on a node: its page, a new tab, or the graph from there', async () => {
    const onNavigate = vi.fn()
    render(<GraphView graph="g" node={rootNode} onNavigateToNode={onNavigate} />)
    await screen.findByTestId('renderer')
    fireEvent.click(screen.getByRole('button', { name: 'count::root::outgoing::has_part' }))
    fireEvent.click(screen.getByRole('button', { name: 'pick' }))
    const child = await screen.findByRole('button', { name: 'child' })

    fireEvent.contextMenu(child)
    const menu = screen.getByRole('menu', { name: 'Child' })
    expect(menu).toHaveStyle({ left: '12px', top: '34px' })
    fireEvent.click(screen.getByRole('menuitem', { name: 'Open in a new tab' }))
    expect(onNavigate).toHaveBeenCalledWith(expect.anything(), { newTab: true })
    expect(onNavigate.mock.calls[0][0].getNodeId()).toBe('child')
    expect(screen.queryByRole('menu')).toBeNull()

    fireEvent.contextMenu(child)
    fireEvent.click(screen.getByRole('menuitem', { name: 'Open node page' }))
    expect(onNavigate).toHaveBeenLastCalledWith(expect.anything(), { newTab: false })

    // ctrl + double click is the keyboard way to a new tab
    fireEvent.doubleClick(child, { ctrlKey: true })
    expect(onNavigate).toHaveBeenLastCalledWith(expect.anything(), { newTab: true })
  })

  it('without a page to go to, the menu explores from the node in place', async () => {
    await renderLoaded()
    fireEvent.click(screen.getByRole('button', { name: 'count::root::outgoing::has_part' }))
    fireEvent.click(screen.getByRole('button', { name: 'pick' }))
    const child = await screen.findByRole('button', { name: 'child' })
    fireEvent.contextMenu(child)
    expect(screen.queryByRole('menuitem', { name: 'Open node page' })).toBeNull()
    fireEvent.click(screen.getByRole('menuitem', { name: 'Explore from here' }))
    await waitFor(() => expect(mockedFetch).toHaveBeenLastCalledWith('g', encodeNodeId('child')))
    expect(screen.queryByRole('menu')).toBeNull()
  })

  it('toggles fullscreen', async () => {
    await renderLoaded()
    fireEvent.click(screen.getByTitle('Fullscreen'))
    expect(screen.getByTitle('Exit fullscreen')).toBeInTheDocument()
    fireEvent.click(screen.getByTitle('Exit fullscreen'))
    expect(screen.getByTitle('Fullscreen')).toBeInTheDocument()
  })
})
