import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom'
import encodeNodeId from '../../../encodeNodeId'

process.env.PUBLIC_URL = '/'

vi.mock('../../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, get: vi.fn(), getPaginated: vi.fn(), post: vi.fn() }
})
// sigma-based; the page only needs to hand it the graph and node
vi.mock('../../../components/node_graph_view/GraphView', () => ({
  default: ({ graph, node }: any) => <div data-testid="graph-view">{graph}:{node.getName()}</div>,
}))
vi.mock('../../../components/node_graph_view/edgeCountsCache', () => ({ prefetchNodeEdgeCounts: vi.fn(), fetchNodeEdgeCounts: vi.fn() }))
// the question carousel has its own timers and fetches; stub it with buttons
// that drive the callback the page cares about
vi.mock('../../../components/query/CyclingQuestions', () => ({
  default: ({ graph, onVisibleSourceIdsChange }: any) => (
    <div data-testid="cycling">
      questions for {graph}
      <button onClick={() => onVisibleSourceIdsChange('mondo:1', 'mondo:2')}>show mondo:1</button>
      <button onClick={() => onVisibleSourceIdsChange('mondo:2', null)}>show mondo:2</button>
      <button onClick={() => onVisibleSourceIdsChange(null, null)}>show nothing</button>
    </div>
  ),
}))

import { get, getPaginated, Page } from '../../../app/api'
import { prefetchNodeEdgeCounts } from '../../../components/node_graph_view/edgeCountsCache'
import EbiHomePage from './EbiHomePage'

const mockedGet = vi.mocked(get)
const mockedGetPaginated = vi.mocked(getPaginated)
const mockedPrefetch = vi.mocked(prefetchNodeEdgeCounts)

const nodes: Record<string, any> = {
  'mondo:1': { 'grebi:nodeId': 'n1', 'grebi:name': ['Node One'] },
  'mondo:2': { 'grebi:nodeId': 'n2', 'grebi:name': ['Node Two'] },
}

function LocationDisplay() {
  const loc = useLocation()
  return <div data-testid="location">{loc.pathname + loc.search}</div>
}

function renderHome() {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <Routes>
        <Route path="/" element={<><EbiHomePage /><LocationDisplay /></>} />
      </Routes>
    </MemoryRouter>
  )
}

beforeEach(() => {
  mockedGet.mockReset()
  mockedGetPaginated.mockReset()
  mockedPrefetch.mockReset()
  mockedGet.mockImplementation(async (path: string) => {
    if (path === 'api/v1/stats') return { g1: { num_nodes: 1000, num_edges: 5000 }, g2: { num_nodes: 7, num_edges: 8 } }
    if (path === 'api/v1/graphs') return ['g1', 'g2']
    if (path === 'api/v1/graphs/g1') return { subgraph_config: { name: 'Graph One' } }
    if (path === 'api/v1/graphs/g2') throw new Error('no metadata')
    if (path.endsWith('/embedding_models')) return []
    throw new Error('unexpected GET ' + path)
  })
  mockedGetPaginated.mockImplementation(async (_path: string, params: any) => {
    const found = nodes[params['grebi:sourceIds']]
    return new Page<any>(0, found ? 1 : 0, 1, found ? 1 : 0, found ? [found] : [], new Map())
  })
})

describe('EbiHomePage', () => {
  it('lists the graphs with names and stats once their metadata loads, selecting the first', async () => {
    const { container } = renderHome()
    expect(container.querySelector('.spinner-default')).not.toBeNull()

    expect(await screen.findByText('Graph One')).toBeInTheDocument()
    expect(document.title).toBe('EMBL-EBI Knowledge Graph')
    expect(screen.getByText('1,000')).toBeInTheDocument()
    expect(screen.getByText('5,000')).toBeInTheDocument()
    // a graph whose metadata failed to load falls back to its id as name
    expect(screen.getAllByTitle('g2')).toHaveLength(2)

    const radios = screen.getAllByRole('radio')
    expect(radios[0]).toBeChecked()
    expect(radios[1]).not.toBeChecked()
    expect(screen.getByPlaceholderText('Search Graph One for knowledge about...')).toBeInTheDocument()
    expect(screen.getByTestId('cycling')).toHaveTextContent('questions for g1')
    expect(screen.getAllByTitle('Graph info')[0].closest('a')).toHaveAttribute('href', '/graphs/g1')
  })

  it('selecting another graph retargets the search box and questions without leaving the page', async () => {
    renderHome()
    await screen.findByText('Graph One')
    fireEvent.click(screen.getAllByRole('radio')[1])

    expect(screen.getAllByRole('radio')[1]).toBeChecked()
    expect(screen.getByPlaceholderText('Search g2 for knowledge about...')).toBeInTheDocument()
    expect(screen.getByTestId('cycling')).toHaveTextContent('questions for g2')
    expect(screen.getByTestId('location')).toHaveTextContent('/')
    expect(screen.queryByTestId('graph-view')).toBeNull()
  })

  it('loads the node behind the visible question by source id, warms the next one, and caches both', async () => {
    renderHome()
    await screen.findByText('Graph One')

    fireEvent.click(screen.getByText('show mondo:1'))
    expect(await screen.findByTestId('graph-view')).toHaveTextContent('g1:Node One')
    expect(mockedGetPaginated).toHaveBeenCalledWith('api/v1/graphs/g1/nodes', { 'grebi:sourceIds': 'mondo:1', size: '1', resolve: 'false' })
    expect(mockedGetPaginated).toHaveBeenCalledWith('api/v1/graphs/g1/nodes', { 'grebi:sourceIds': 'mondo:2', size: '1', resolve: 'false' })
    await waitFor(() => expect(mockedPrefetch).toHaveBeenCalledWith('g1', encodeNodeId('n2')))
    expect(mockedPrefetch).toHaveBeenCalledWith('g1', encodeNodeId('n1'))
    expect(mockedGetPaginated).toHaveBeenCalledTimes(2)

    // both nodes are now cached: switching between them needs no more requests
    fireEvent.click(screen.getByText('show mondo:2'))
    expect(await screen.findByTestId('graph-view')).toHaveTextContent('g1:Node Two')
    fireEvent.click(screen.getByText('show mondo:1'))
    expect(await screen.findByTestId('graph-view')).toHaveTextContent('g1:Node One')
    expect(mockedGetPaginated).toHaveBeenCalledTimes(2)

    fireEvent.click(screen.getByText('show nothing'))
    expect(screen.queryByTestId('graph-view')).toBeNull()
  })

  it('shows nothing for a source id that does not resolve to a node', async () => {
    mockedGetPaginated.mockResolvedValue(new Page<any>(0, 0, 0, 0, [], new Map()))
    renderHome()
    await screen.findByText('Graph One')
    fireEvent.click(screen.getByText('show mondo:1'))
    await waitFor(() => expect(mockedGetPaginated).toHaveBeenCalled())
    expect(screen.queryByTestId('graph-view')).toBeNull()
  })
})
