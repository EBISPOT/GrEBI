import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, within } from '@testing-library/react'
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom'
import EbiGraphPage from './EbiGraphPage'

vi.mock('../../../app/api', async (importOriginal) => ({ ...(await importOriginal<any>()), get: vi.fn(), getPaginated: vi.fn(), post: vi.fn() }))
import { get } from '../../../app/api'
const mockedGet = vi.mocked(get)

const meta = {
  subgraph_config: {
    id: 'g1',
    name: 'Graph One',
    datasource_configs: [{ id: 'OLS.mondo', description: 'Mondo ontology' }, { id: 'gwas' }],
  },
}
const dist = {
  node_counts_by_datasource: { 'OLS.mondo': 100, gwas: 50 },
  node_counts_by_type: { 'biolink:Gene': 30, 'biolink:Disease': 70 },
  edge_counts_by_datasource: { gwas: 40 },
  edge_counts_by_type: { is_a: 60, has_part: 5 },
}

let distStatsFail = false

function LocationDisplay() {
  const loc = useLocation()
  return <div data-testid="location">{loc.pathname + loc.search}</div>
}

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/graphs/g1']}>
      <Routes>
        <Route path="/graphs/:graph" element={<><EbiGraphPage /><LocationDisplay /></>} />
        <Route path="*" element={<LocationDisplay />} />
      </Routes>
    </MemoryRouter>
  )
}

beforeEach(() => {
  distStatsFail = false
  mockedGet.mockReset()
  mockedGet.mockImplementation(async (path: string) => {
    if (path === 'api/v1/graphs/g1') return meta
    if (path === 'api/v1/stats') return { g1: { num_nodes: 12345, num_edges: 678 } }
    if (path === 'api/v1/graphs/g1/stats') {
      if (distStatsFail) throw new Error('no stats')
      return dist
    }
    if (path === 'api/v1/graphs/g1/embedding_models') return []
    if (path === 'api/v1/graphs') return ['g1']
    throw new Error('unexpected GET ' + path)
  })
})

describe('EbiGraphPage', () => {
  it('shows a spinner until the metadata arrives, then the summary and datasource table', async () => {
    const { container } = renderPage()
    expect(container.querySelector('.spinner-default')).not.toBeNull()

    expect(await screen.findByText('Graph One')).toBeInTheDocument()
    expect(document.title).toBe('g1 - GrEBI')
    expect(screen.getByText('12,345')).toBeInTheDocument()
    expect(screen.getByText('678')).toBeInTheDocument()
    expect(screen.getByText('Datasources', { selector: 'td' }).nextElementSibling).toHaveTextContent('2')
    expect(screen.getByPlaceholderText('Search Graph One...')).toBeInTheDocument()

    expect(screen.getByText('OLS.mondo').nextElementSibling).toHaveTextContent('Mondo ontology')
    expect(screen.getByText('gwas').nextElementSibling).toHaveTextContent('—')
    expect(screen.getByRole('link', { name: 'Graphs' })).toHaveAttribute('href', '/graphs')
  })

  it('lists node and edge types by count, each linking to a filtered search', async () => {
    renderPage()
    await screen.findByText('Graph One')

    fireEvent.click(screen.getByRole('button', { name: 'Node Types' }))
    const typeButtons = await screen.findAllByRole('button', { name: /^biolink:/ })
    expect(typeButtons.map((b) => b.textContent)).toEqual(['biolink:Disease', 'biolink:Gene'])
    expect(screen.getByText('70')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'biolink:Gene' }))
    expect(screen.getByTestId('location')).toHaveTextContent('/graphs/g1/search?q=*&grebi%3Atype=biolink%3AGene')
  })

  it('edge types link to the edge search', async () => {
    renderPage()
    await screen.findByText('Graph One')
    fireEvent.click(screen.getByRole('button', { name: 'Edge Types' }))
    const typeButtons = await screen.findAllByRole('button', { name: /^(is_a|has_part)$/ })
    expect(typeButtons.map((b) => b.textContent)).toEqual(['is_a', 'has_part'])
    fireEvent.click(screen.getByRole('button', { name: 'has_part' }))
    expect(screen.getByTestId('location')).toHaveTextContent('/graphs/g1/edges?grebi%3Atype=has_part')
  })

  it('pie chart legends navigate to node or edge search filtered by datasource', async () => {
    renderPage()
    await screen.findByText('Node Datasources')
    const nodeChart = within(screen.getByText('Node Datasources').parentElement!)
    fireEvent.click(nodeChart.getByTitle('OLS.mondo'))
    expect(screen.getByTestId('location')).toHaveTextContent('/graphs/g1/search?q=*&grebi%3Adatasources=OLS.mondo')
  })

  it('edge datasource legends navigate to the edge search', async () => {
    renderPage()
    await screen.findByText('Edge Datasources')
    const edgeChart = within(screen.getByText('Edge Datasources').parentElement!)
    fireEvent.click(edgeChart.getByTitle('gwas'))
    expect(screen.getByTestId('location')).toHaveTextContent('/graphs/g1/edges?grebi%3Adatasources=gwas')
  })

  it('keeps spinners in the charts and type tabs when distribution stats are unavailable', async () => {
    distStatsFail = true
    const { container } = renderPage()
    await screen.findByText('Graph One')
    expect(screen.queryByText('Node Datasources')).toBeNull()
    expect(container.querySelectorAll('.spinner-default').length).toBeGreaterThan(0)

    fireEvent.click(screen.getByRole('button', { name: 'Node Types' }))
    expect(screen.queryByRole('button', { name: /^biolink:/ })).toBeNull()
    expect(container.querySelectorAll('.spinner-default').length).toBe(2)
  })
})

describe('EbiGraphPage errors', () => {
  it('says when there is no such graph', async () => {
    const { ApiError } = await import('../../../app/api')
    mockedGet.mockImplementation(async (path: string) => {
      if (path === 'api/v1/graphs/g1') throw new ApiError(404, path, 'Unknown graph g1')
      if (path === 'api/v1/stats') return {}
      if (path === 'api/v1/graphs/g1/stats') return dist
      if (path === 'api/v1/graphs/g1/embedding_models') return []
      if (path === 'api/v1/graphs') return ['g1']
      throw new Error('unexpected GET ' + path)
    })
    renderPage()
    const alert = await screen.findByRole('alert')
    expect(alert).toHaveTextContent('The graph g1 not found')
    expect(alert).toHaveTextContent('Unknown graph g1')
    expect(document.querySelector('.spinner-default')).toBeNull()
  })
})
