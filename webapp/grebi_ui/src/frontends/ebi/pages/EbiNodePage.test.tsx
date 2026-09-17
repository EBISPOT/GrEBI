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
  default: ({ graph, node, exploration, onExplorationChange, onNavigateToNode }: any) => (
    <div data-testid="graph-view" data-exploration={exploration ?? ''}>
      {graph}:{node.getName()}
      <button onClick={() => onExplorationChange('abc', { replace: false })}>explore</button>
      <button onClick={() => onExplorationChange('def', { replace: true })}>filter</button>
      <button onClick={() => onExplorationChange(null, { replace: false })}>reset</button>
      <button onClick={() => onNavigateToNode({ getEncodedNodeId: () => encodeNodeId('mondo:1') })}>go child</button>
    </div>
  ),
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

  it('offers the languages a translated node has, and reads it in the one chosen', async () => {
    // values as the API serves them: wrapped with their provenance, a translation reified with its language
    const merged = (value: any) => ({ 'grebi:datasources': ['OLS.mondo'], 'grebi:sourceIds': [nodeId], 'grebi:value': value })
    const translated = (text: string, lang: string) => merged({ 'grebi:value': text, 'grebi:properties': { 'grebi:lang': [lang] } })
    mockedGet.mockImplementation(async (path: string) => {
      if (path.startsWith('api/v1/graphs/g1/nodes/')) return {
        ...nodeProps,
        'grebi:name': [merged('psoriasis'), translated('psoriasis (fr)', 'fr')],
        'grebi:description': [merged('A skin disease'), translated('Une maladie de la peau', 'fr')],
        'grebi:languages': ['en', 'fr'],
        '_refs': {},
      }
      if (path === 'api/v1/graphs/g1/embedding_models') return models
      if (path === 'api/v1/graphs') return ['g1']
      if (path === 'api/v1/stats') return {}
      throw new Error('unexpected GET ' + path)
    })
    renderPage()
    expect(await screen.findByRole('heading', { name: /psoriasis/ })).toHaveTextContent('psoriasis Disease')
    const picker = screen.getByRole('combobox', { name: 'Language' }) as HTMLSelectElement
    expect(Array.from(picker.options).map(o => o.textContent)).toEqual(['English', 'French'])
    expect(picker.value).toBe('en')

    fireEvent.change(picker, { target: { value: 'fr' } })
    expect(screen.getByTestId('location')).toHaveTextContent('?lang=fr')
    await waitFor(() => expect(mockedGet).toHaveBeenCalledWith(`api/v1/graphs/g1/nodes/${encodeNodeId(nodeId)}?lang=fr`))
    expect(await screen.findByRole('heading', { name: /psoriasis \(fr\)/ })).toBeInTheDocument()
    expect(screen.getByText('Une maladie de la peau')).toBeInTheDocument()

    // the language survives a change of tab, and the tab a change of language
    fireEvent.click(screen.getByRole('tab', { name: 'Property View' }))
    expect(screen.getByTestId('location')).toHaveTextContent('?lang=fr&tab=properties')
    fireEvent.change(screen.getByRole('combobox', { name: 'Language' }), { target: { value: 'en' } })
    expect(screen.getByTestId('location')).toHaveTextContent('?lang=en&tab=properties')
  })

  it('says when there is no such node, keeping the search box', async () => {
    const { ApiError } = await import('../../../app/api')
    mockedGet.mockImplementation(async (path: string) => {
      if (path.startsWith('api/v1/graphs/g1/nodes/')) throw new ApiError(404, path, 'Node not found')
      if (path === 'api/v1/graphs/g1/embedding_models') return models
      if (path === 'api/v1/graphs') return ['g1']
      if (path === 'api/v1/stats') return {}
      throw new Error('unexpected GET ' + path)
    })
    renderPage()
    expect(await screen.findByRole('alert')).toHaveTextContent(`No node with the id ${nodeId} in g1`)
    expect(screen.getByPlaceholderText(/Search for knowledge/)).toBeInTheDocument()
    expect(screen.queryByText('Loading node...')).toBeNull()
  })

  it('shows the error when the node cannot be loaded', async () => {
    const { ApiError } = await import('../../../app/api')
    mockedGet.mockImplementation(async (path: string) => {
      if (path.startsWith('api/v1/graphs/g1/nodes/')) throw new ApiError(500, path, 'database down')
      if (path === 'api/v1/graphs/g1/embedding_models') return models
      if (path === 'api/v1/graphs') return ['g1']
      if (path === 'api/v1/stats') return {}
      throw new Error('unexpected GET ' + path)
    })
    renderPage()
    const alert = await screen.findByRole('alert')
    expect(alert).toHaveTextContent('The node could not be loaded')
    expect(alert).toHaveTextContent('database down (HTTP 500)')
    expect(screen.queryByText('Loading node...')).toBeNull()
  })

  it('keeps the graph exploration in the URL and goes to a node the graph asks for', async () => {
    renderPage('?tab=graph&g=xyz')
    expect(await screen.findByTestId('graph-view')).toHaveAttribute('data-exploration', 'xyz')

    fireEvent.click(screen.getByRole('button', { name: 'explore' }))
    expect(screen.getByTestId('location')).toHaveTextContent('?tab=graph&g=abc')
    fireEvent.click(screen.getByRole('button', { name: 'filter' }))
    expect(screen.getByTestId('location')).toHaveTextContent('?tab=graph&g=def')
    fireEvent.click(screen.getByRole('button', { name: 'reset' }))
    expect(screen.getByTestId('location')).toHaveTextContent('?tab=graph')
    expect(screen.getByTestId('location')).not.toHaveTextContent('g=')

    fireEvent.click(screen.getByRole('button', { name: 'go child' }))
    expect(screen.getByTestId('location')).toHaveTextContent(`/graphs/g1/nodes/${encodeNodeId('mondo:1')}?tab=graph`)
  })

  it('has no language picker for a node in one language', async () => {
    renderPage()
    await screen.findByText('A skin disease')
    expect(screen.queryByRole('combobox', { name: 'Language' })).toBeNull()
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
