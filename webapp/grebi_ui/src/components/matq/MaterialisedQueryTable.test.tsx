import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import MaterialisedQueryTable from './MaterialisedQueryTable'

const api = vi.hoisted(() => ({ get: vi.fn() }))

vi.mock('../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, get: api.get }
})

function LocationDisplay() {
  const loc = useLocation()
  return <div data-testid="location">{loc.pathname + loc.search}</div>
}

const matqs = [
  { id: 'gwas_by_disease', graph: 'g', description: 'GWAS studies by disease', end_time: '2026-01-02' },
  { id: 'impc_x_gwas', graph: 'other', description: 'IMPC x GWAS', end_time: '2026-01-03' },
]

function renderTable(graph?: string) {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <MaterialisedQueryTable graph={graph} />
      <LocationDisplay />
    </MemoryRouter>
  )
}

beforeEach(() => {
  api.get.mockReset()
  api.get.mockImplementation(async (path: string) =>
    path.endsWith('/materialised_queries') ? matqs : { materialised_queries: matqs }
  )
})

describe('MaterialisedQueryTable', () => {
  it('shows a spinner until the queries and the graph metadata arrive, then a row per query', async () => {
    renderTable('g')
    expect(screen.getByRole('progressbar')).toBeInTheDocument()

    expect(await screen.findByText('gwas_by_disease')).toBeInTheDocument()
    expect(screen.queryByRole('progressbar')).toBeNull()
    expect(api.get).toHaveBeenCalledWith('api/v1/graphs/g/materialised_queries')
    expect(api.get).toHaveBeenCalledWith('api/v1/graphs/g')
    for (const header of ['Query ID', 'Description', 'Updated']) {
      expect(screen.getByText(header)).toBeInTheDocument()
    }
    expect(screen.getByText('GWAS studies by disease')).toBeInTheDocument()
    expect(screen.getByText('2026-01-03')).toBeInTheDocument()
  })

  it('links each query to its template source and to the CSV downloads', async () => {
    const { container } = renderTable('g')
    await screen.findByText('gwas_by_disease')
    const source = container.querySelector(
      'a[href="https://github.com/EBISPOT/GrEBI/blob/dev/query_templates/gwas_by_disease.yaml"]'
    )
    expect(source).not.toBeNull()
    expect(source).toHaveAttribute('target', '_blank')

    const downloads = screen.getAllByRole('link', { name: /CSV/ })
    expect(downloads).toHaveLength(2)
    expect(downloads[0]).toHaveAttribute('href', 'https://ftp.ebi.ac.uk/pub/databases/spot/kg/')
  })

  it('clicking a row opens that query on its own graph', async () => {
    renderTable('g')
    await screen.findByText('impc_x_gwas')
    fireEvent.click(screen.getByText('IMPC x GWAS'))
    expect(screen.getByTestId('location')).toHaveTextContent('/graphs/other/tables/impc_x_gwas')
    fireEvent.click(screen.getByText('GWAS studies by disease'))
    expect(screen.getByTestId('location')).toHaveTextContent('/graphs/g/tables/gwas_by_disease')
  })

  it('filters the rows locally through the search box', async () => {
    renderTable('g')
    await screen.findByText('impc_x_gwas')
    fireEvent.change(screen.getByPlaceholderText('Search...'), { target: { value: 'impc' } })
    expect(screen.getByText('impc_x_gwas')).toBeInTheDocument()
    expect(screen.queryByText('gwas_by_disease')).toBeNull()
    expect(api.get).toHaveBeenCalledTimes(2)
  })

  it('without a graph it lists the queries of every graph and skips the metadata', async () => {
    renderTable(undefined)
    expect(await screen.findByText('gwas_by_disease')).toBeInTheDocument()
    expect(api.get).toHaveBeenCalledTimes(1)
    expect(api.get).toHaveBeenCalledWith('api/v1/materialised_queries')
  })
})
