import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react'
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom'
import encodeNodeId from '../../../encodeNodeId'
import EbiEdgeSearchPage from './EbiEdgeSearchPage'

vi.mock('../../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, get: vi.fn(), getPaginated: vi.fn(), post: vi.fn() }
})
// the node selector searches as you type; here it is a button that picks a fixed node
vi.mock('../../../components/NodeSelectorBox', async () => {
  const { default: GraphNodeRef } = await import('../../../model/GraphNodeRef')
  return {
    default: ({ placeholder, selectedNode, onNodeSelect, onClear }: any) => (
      <div>
        <span data-testid={`selected-${placeholder}`}>{selectedNode ? selectedNode.getName() : ''}</span>
        <button onClick={() => onNodeSelect(new GraphNodeRef({ 'grebi:nodeId': 'a', 'grebi:name': ['Alpha'] }))}>{`pick ${placeholder}`}</button>
        <button onClick={onClear}>{`clear ${placeholder}`}</button>
      </div>
    ),
  }
})
import { get, getPaginated, Page } from '../../../app/api'
const mockedGet = vi.mocked(get)
const mockedGetPaginated = vi.mocked(getPaginated)

const alpha = { 'grebi:nodeId': 'a', 'grebi:name': ['Alpha'], 'grebi:type': ['biolink:Gene'] }
const beta = { 'grebi:nodeId': 'b', 'grebi:name': ['Beta'] }
const edges = [
  { 'grebi:edgeId': 'e1', 'grebi:type': 'is_a', 'grebi:datasources': ['gwas'], from: alpha, to: beta },
  { 'grebi:edgeId': 'e2', 'grebi:type': 'part_of', 'grebi:datasources': ['gwas'], 'grebi:fromNodeId': 'raw:from', to: beta },
]
let total = 45
let results = edges

function LocationDisplay() {
  const loc = useLocation()
  return <div data-testid="location">{loc.pathname + loc.search}</div>
}

function renderPage(search = '') {
  return render(
    <MemoryRouter initialEntries={[`/graphs/g1/edges${search}`]}>
      <Routes>
        <Route path="/graphs/:graph/edges" element={<><EbiEdgeSearchPage /><LocationDisplay /></>} />
      </Routes>
    </MemoryRouter>
  )
}

function lastRequest() {
  const url = new URL(mockedGetPaginated.mock.calls.at(-1)![0], 'http://x/')
  return { pathname: url.pathname, params: url.searchParams }
}

beforeEach(() => {
  total = 45
  results = edges
  mockedGet.mockReset()
  mockedGetPaginated.mockReset()
  mockedGet.mockImplementation(async (path: string) => {
    if (path === 'api/v1/graphs/g1/stats') {
      return { edge_counts_by_type: { part_of: 5, is_a: 10 }, edge_counts_by_datasource: { gwas: 15 } }
    }
    if (path === 'api/v1/graphs') return ['g1']
    if (path === 'api/v1/stats') return {}
    if (path === `api/v1/graphs/g1/nodes/${encodeNodeId('a')}`) return alpha
    throw new Error('unexpected GET ' + path)
  })
  mockedGetPaginated.mockImplementation(async () => new Page<any>(0, results.length, Math.ceil(total / 20), total, results, {} as any))
})

describe('EbiEdgeSearchPage', () => {
  it('searches edges for the graph and lists them with node links, type and datasources', async () => {
    renderPage()
    expect(screen.getByText('Searching edges...')).toBeInTheDocument()

    expect(await screen.findByRole('link', { name: /Alpha/ })).toHaveAttribute('href', `/graphs/g1/nodes/${encodeNodeId('a')}`)
    expect(screen.getByRole('link', { name: /Alpha/ })).toHaveTextContent('Gene')
    expect(screen.getByText('45 results')).toBeInTheDocument()
    expect(screen.getAllByRole('link', { name: 'Beta' })).toHaveLength(2)
    // an edge without a resolved source node shows the raw id
    expect(screen.getByText('raw:from')).toBeInTheDocument()
    expect(screen.getAllByTitle('gwas').length).toBeGreaterThanOrEqual(2)

    const { pathname, params } = lastRequest()
    expect(pathname).toBe('/api/v1/graphs/g1/edges')
    expect(Object.fromEntries(params)).toEqual({ page: '0', size: '20', sortBy: 'grebi:type', sortDir: 'asc' })
  })

  it('offers facets from the graph stats when the search returns none, most frequent first', async () => {
    renderPage()
    await screen.findByText('45 results')
    // the results table has an "Edge Type" column header too; the facet titles are divs
    const typeFacet = within(screen.getByText('Edge Type', { selector: 'div' }).parentElement!)
    await waitFor(() => expect(typeFacet.getAllByRole('button').map((b) => b.textContent)).toEqual(['is_a(10)', 'part_of(5)']))
    const dsFacet = within(screen.getByText('Datasource', { selector: 'div' }).parentElement!)
    expect(dsFacet.getByRole('button')).toHaveTextContent('gwas(15)')

    fireEvent.click(screen.getByRole('button', { name: 'Filters' }))
    expect(screen.queryByText('Edge Type', { selector: 'div' })).toBeNull()
  })

  it('clicking a facet filters through the URL, shows a chip, and the chip clears it again', async () => {
    renderPage()
    await screen.findByText('45 results')
    fireEvent.click(await screen.findByRole('button', { name: /is_a/ }))

    expect(screen.getByTestId('location')).toHaveTextContent('/graphs/g1/edges?grebi%3Atype=is_a')
    await waitFor(() => expect(lastRequest().params.get('grebi:type')).toBe('is_a'))
    const chip = screen.getByText(/Type: is_a/)
    fireEvent.click(within(chip).getByRole('button'))
    expect(screen.getByTestId('location')).toHaveTextContent('/graphs/g1/edges')
    await waitFor(() => expect(lastRequest().params.get('grebi:type')).toBeNull())
  })

  it('applies filters already in the URL and lets a datasource filter be chosen too', async () => {
    renderPage('?grebi%3Atype=part_of')
    await screen.findByText('45 results')
    expect(lastRequest().params.get('grebi:type')).toBe('part_of')
    expect(screen.getByText(/Type: part_of/)).toBeInTheDocument()

    const dsFacetTitle = await screen.findByText('Datasource', { selector: 'div' })
    fireEvent.click(within(dsFacetTitle.parentElement!).getByRole('button'))
    await waitFor(() => expect(lastRequest().params.get('grebi:datasources')).toBe('gwas'))
    expect(lastRequest().params.get('grebi:type')).toBe('part_of')
    expect(screen.getByText(/Datasource: gwas/)).toBeInTheDocument()
  })

  it('pages through the results', async () => {
    renderPage()
    await screen.findByText('45 results')
    const pagination = screen.getByRole('navigation', { name: 'pagination navigation' })
    expect(within(pagination).getByRole('button', { name: 'Go to page 3' })).toBeInTheDocument()
    fireEvent.click(within(pagination).getByRole('button', { name: 'Go to page 2' }))
    await waitFor(() => expect(lastRequest().params.get('page')).toBe('1'))
  })

  it('says when nothing matches', async () => {
    results = []
    total = 0
    renderPage()
    expect(await screen.findByText('No edges found.')).toBeInTheDocument()
    expect(screen.queryByText(/results/)).toBeNull()
  })
})

describe('EbiEdgeSearchPage edge details', () => {
  it('opens the properties of an edge from its row', async () => {
    renderPage()
    const button = await screen.findByRole('button', { name: 'View edge e1' })
    mockedGet.mockImplementation(async (path: string) => {
      if (path === `api/v1/graphs/g1/edges/${encodeNodeId('e1')}`) return { 'grebi:edgeId': 'e1', 'grebi:type': 'is_a', 'grebi:datasources': ['gwas'], from: alpha, to: beta, 'gwas:pvalue': ['1e-8'], _refs: {} }
      return { edge_counts_by_type: {}, edge_counts_by_datasource: {} }
    })
    fireEvent.click(button)
    expect(await screen.findByText('Edge Properties')).toBeInTheDocument()
    expect(await screen.findByText(/1e-8/)).toBeInTheDocument()
    expect(screen.getByRole('link', { name: /Open edge page/ })).toHaveAttribute('href', `/graphs/g1/edges/${encodeNodeId('e1')}`)
  })
})

describe('EbiEdgeSearchPage filters', () => {
  it('takes any number of types and datasources as alternatives, each removable by its chip', async () => {
    renderPage()
    await screen.findByRole('link', { name: /Alpha/ })
    const typeFacet = within(screen.getByText('Edge Type', { selector: 'div' }).parentElement!)
    await waitFor(() => expect(typeFacet.getAllByRole('button')).toHaveLength(2))
    fireEvent.click(typeFacet.getByRole('button', { name: /is_a/ }))
    fireEvent.click(typeFacet.getByRole('button', { name: /part_of/ }))
    fireEvent.click(within(screen.getByText('Datasource', { selector: 'div' }).parentElement!).getByRole('button', { name: /gwas/ }))
    expect(screen.getByTestId('location')).toHaveTextContent('grebi%3Atype=is_a&grebi%3Atype=part_of&grebi%3Adatasources=gwas')
    await waitFor(() => expect(lastRequest().params.getAll('grebi:type')).toEqual(['is_a', 'part_of']))
    expect(lastRequest().params.getAll('grebi:datasources')).toEqual(['gwas'])
    expect(typeFacet.getByRole('button', { name: /is_a/ })).toHaveAttribute('aria-pressed', 'true')

    fireEvent.click(screen.getByRole('button', { name: 'Remove Type is_a' }))
    expect(screen.getByTestId('location')).toHaveTextContent('grebi%3Atype=part_of')
    expect(screen.getByTestId('location')).toHaveTextContent('grebi%3Adatasources=gwas')
    expect(screen.getByTestId('location')).not.toHaveTextContent('is_a')
  })

  it('narrows to the edges of a chosen end node, named in the chip', async () => {
    renderPage()
    await screen.findByRole('link', { name: /Alpha/ })
    fireEvent.click(screen.getAllByRole('button', { name: 'pick Any node' })[0])
    expect(screen.getByTestId('location')).toHaveTextContent('grebi%3AfromNodeId=a')
    await waitFor(() => expect(lastRequest().params.get('grebi:fromNodeId')).toBe('a'))
    expect(await screen.findByRole('button', { name: 'Remove From Alpha' })).toBeInTheDocument()
    expect(screen.getAllByTestId('selected-Any node')[0]).toHaveTextContent('Alpha')

    fireEvent.click(screen.getByRole('button', { name: 'Remove From Alpha' }))
    expect(screen.getByTestId('location')).not.toHaveTextContent('fromNodeId')
  })

  it('sorts by type in either direction from the column header', async () => {
    renderPage()
    await screen.findByRole('link', { name: /Alpha/ })
    expect(lastRequest().params.get('sortDir')).toBe('asc')
    fireEvent.click(screen.getByRole('button', { name: /Edge Type/ }))
    expect(screen.getByTestId('location')).toHaveTextContent('sortDir=desc')
    await waitFor(() => expect(lastRequest().params.get('sortDir')).toBe('desc'))
    expect(lastRequest().params.get('sortBy')).toBe('grebi:type')
  })
})
