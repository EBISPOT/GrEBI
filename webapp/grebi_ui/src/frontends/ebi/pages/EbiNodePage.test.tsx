import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react'
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom'
import encodeNodeId from '../../../encodeNodeId'
import EbiNodePage from './EbiNodePage'

vi.mock('../../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, get: vi.fn(), getPaginated: vi.fn(), post: vi.fn() }
})
vi.mock('../../../components/node_graph_view/GraphView', () => ({
  default: ({ graph, node }: any) => <div data-testid="graph-view">{graph}:{node.getName()}</div>,
}))

import { get, getPaginated, Page } from '../../../app/api'
const mockedGet = vi.mocked(get)
const mockedGetPaginated = vi.mocked(getPaginated)

const nodeId = 'mondo:0005083'
const nodeProps = {
  'grebi:nodeId': nodeId,
  'grebi:name': ['psoriasis'],
  'grebi:description': ['A skin disease'],
  'grebi:type': ['biolink:Disease', 'ols:Class'],
  'grebi:sourceIds': ['mondo:0005083', 'doid:8893'],
  'grebi:datasources': ['OLS.mondo'],
}
const incomingEdge = {
  'grebi:edgeId': 'e1',
  'grebi:type': 'is_a',
  'grebi:datasources': ['OLS.mondo'],
  from: { 'grebi:nodeId': 'mondo:1', 'grebi:name': ['plaque psoriasis'] },
  to: nodeProps,
}

let models: any[] = []

function LocationDisplay() {
  const loc = useLocation()
  return <div data-testid="location">{loc.pathname + loc.search}</div>
}

function renderPage(search = '') {
  return render(
    <MemoryRouter initialEntries={[`/graphs/g1/nodes/${encodeNodeId(nodeId)}${search}`]}>
      <Routes>
        <Route path="/graphs/:graph/nodes/:nodeId" element={<><EbiNodePage /><LocationDisplay /></>} />
      </Routes>
    </MemoryRouter>
  )
}

beforeEach(() => {
  models = []
  mockedGet.mockReset()
  mockedGetPaginated.mockReset()
  mockedGet.mockImplementation(async (path: string) => {
    if (path.startsWith('api/v1/graphs/g1/nodes/')) return nodeProps
    if (path === 'api/v1/graphs/g1/embedding_models') return models
    if (path === 'api/v1/graphs') return ['g1']
    if (path === 'api/v1/stats') return {}
    throw new Error('unexpected GET ' + path)
  })
  mockedGetPaginated.mockResolvedValue(new Page<any>(0, 1, 1, 1, [incomingEdge], { 'grebi:datasources': { 'OLS.mondo': 1 } } as any))
})

describe('EbiNodePage', () => {
  it('fetches the node named in the URL and renders its name, type, description and ids', async () => {
    renderPage()
    expect(screen.getByText('Loading node...')).toBeInTheDocument()

    expect(await screen.findByRole('heading', { name: /psoriasis/ })).toHaveTextContent('psoriasis Disease')
    expect(mockedGet).toHaveBeenCalledWith(`api/v1/graphs/g1/nodes/${encodeNodeId(nodeId)}?lang=en`)
    expect(screen.getByText('A skin disease')).toBeInTheDocument()
    // the source ids link out to their databases, with the database icon
    const mondo = screen.getByRole('link', { name: 'mondo:0005083' })
    expect(mondo).toHaveAttribute('href', 'https://www.ebi.ac.uk/ols4/ontologies/mondo/classes?iri=http://purl.obolibrary.org/obo/MONDO_0005083')
    expect(mondo.querySelector('img')).toHaveAttribute('src', expect.stringContaining('db_icons/ols.png'))
    expect(screen.getByRole('link', { name: 'doid:8893' })).toHaveAttribute('href', expect.stringContaining('ols4/ontologies/doid'))

    const crumbs = within(screen.getByLabelText('breadcrumb'))
    expect(crumbs.getAllByRole('link').map((a) => a.getAttribute('href'))).toEqual([
      '/', '/graphs', '/graphs/g1', `/graphs/g1/nodes/${encodeNodeId(nodeId)}`,
    ])
    expect(crumbs.getByRole('link', { name: 'psoriasis' })).toBeInTheDocument()
  })

  it('passes the lang query parameter on to the API', async () => {
    renderPage('?lang=fr')
    await screen.findByText('A skin disease')
    expect(mockedGet).toHaveBeenCalledWith(`api/v1/graphs/g1/nodes/${encodeNodeId(nodeId)}?lang=fr`)
  })

  it('opens the graph tab by default and switches tabs through the URL', async () => {
    renderPage()
    expect(await screen.findByTestId('graph-view')).toHaveTextContent('g1:psoriasis')

    fireEvent.click(screen.getByRole('tab', { name: 'Property View' }))
    expect(screen.getByTestId('location')).toHaveTextContent('?tab=properties')
    expect(screen.queryByTestId('graph-view')).toBeNull()
    // the property table lists the node's datasources
    expect(screen.getByTitle('mondo')).toBeInTheDocument()
  })

  it('loads incoming edges for the Edges In tab', async () => {
    renderPage('?tab=edges_in')
    await screen.findByText('A skin disease')
    expect(await screen.findByRole('link', { name: 'plaque psoriasis' })).toHaveAttribute('href', `/graphs/g1/nodes/${encodeNodeId('mondo:1')}`)
    const path = mockedGetPaginated.mock.calls[0][0]
    expect(path).toContain(`/nodes/${encodeNodeId(nodeId)}/incoming_edges?`)
    expect(screen.queryByTestId('graph-view')).toBeNull()
  })

  it('only offers the Similar tab when the graph has embedding models', async () => {
    const { unmount } = renderPage()
    await screen.findByText('A skin disease')
    expect(screen.queryByRole('tab', { name: 'Similar' })).toBeNull()
    unmount()

    models = [{ model: 'minilm', can_embed: true }]
    renderPage()
    await screen.findByText('A skin disease')
    await waitFor(() => expect(screen.getByRole('tab', { name: 'Similar' })).toBeInTheDocument())
  })
})
