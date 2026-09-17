import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, act, waitFor } from '@testing-library/react'
import GraphNode from '../../model/GraphNode'
import encodeNodeId from '../../encodeNodeId'
import GraphView from './GraphView'

// GraphRenderer wraps sigma (WebGL). Replace it with a stub that exposes the
// layout as buttons so the click/hover wiring in GraphView can be exercised.
vi.mock('./GraphRenderer', () => ({
  default: (props: any) => (
    <div data-testid="renderer" data-ds={props.highlightedDatasource ?? ''} data-et={props.highlightedEdgeType ?? ''}>
      {props.layout.nodes.map((n: any) => (
        <button
          key={n.id}
          onClick={() => {
            if (n.type === 'root') props.onClickRoot()
            else if (n.type === 'count') props.onClickCountNode(n.parentNodeId, n.parentEncodedNodeId, n.direction, n.edgeType)
            else if (n.type === 'expanded_node') props.onClickExpandedNode(n.parentNodeId, n.parentEncodedNodeId, n.direction, n.edgeType)
            else props.onClickAutoExpandedNode(n.parentNodeId, n.parentEncodedNodeId, n.direction, n.edgeType)
          }}
          onDoubleClick={() => props.onDoubleClickExpandedNode(n.parentNodeId, n.direction, n.edgeType, n.id)}
          onMouseEnter={() => props.onHoverNode?.(n)}
          onMouseLeave={() => props.onLeaveNode?.()}
        >
          {n.id}
        </button>
      ))}
    </div>
  ),
}))

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
import { get } from '../../app/api'
import { serialiseExploration } from './exploration'
const mockedFetch = vi.mocked(fetchNodeEdgeCounts)
const mockedGet = vi.mocked(get)

const rootNode = new GraphNode({ 'grebi:nodeId': 'root', 'grebi:name': ['Root'] })
const counts: Record<string, any> = {
  root: { incoming: { is_a: { 'OLS.mondo': 5 } }, outgoing: { has_part: { ds1: 3 } } },
  child: { incoming: {}, outgoing: { regulates: { ds1: 2 } } },
}

beforeEach(() => {
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
    mockedGet.mockImplementation(async (path: string) => ({ 'grebi:nodeId': atob(path.split('/').pop()!), 'grebi:name': ['Child'] }))
    const onChange = vi.fn()
    const token = serialiseExploration({ steps: [{ direction: 'outgoing', edgeType: 'has_part', nodeId: 'child' }], excludedDatasources: [], hiddenEdgeTypes: [] })
    render(<GraphView graph="g" node={rootNode} exploration={token} onExplorationChange={onChange} />)

    // the chain is rebuilt from the URL: the child is fetched and expanded
    expect(await screen.findByRole('button', { name: 'child' })).toBeInTheDocument()
    expect(await screen.findByRole('button', { name: 'count::child::outgoing::regulates' })).toBeInTheDocument()
    expect(mockedGet).toHaveBeenCalledWith(`api/v1/graphs/g/nodes/${encodeNodeId('child')}`)
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

  it('toggles fullscreen', async () => {
    await renderLoaded()
    fireEvent.click(screen.getByTitle('Fullscreen'))
    expect(screen.getByTitle('Exit fullscreen')).toBeInTheDocument()
    fireEvent.click(screen.getByTitle('Exit fullscreen'))
    expect(screen.getByTitle('Fullscreen')).toBeInTheDocument()
  })
})
