import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, within } from '@testing-library/react'
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom'
import EbiTablesHomePage from './EbiTablesHomePage'

vi.mock('../../../app/api', () => ({ get: vi.fn(), getPaginated: vi.fn(), post: vi.fn() }))
import { get } from '../../../app/api'
const mockedGet = vi.mocked(get)

function LocationDisplay() {
  const loc = useLocation()
  return <div data-testid="location">{loc.pathname + loc.search}</div>
}

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/graphs/g1/tables']}>
      <Routes>
        <Route path="/graphs/:graph/tables" element={<><EbiTablesHomePage /><LocationDisplay /></>} />
        <Route path="*" element={<LocationDisplay />} />
      </Routes>
    </MemoryRouter>
  )
}

beforeEach(() => {
  mockedGet.mockReset()
  mockedGet.mockImplementation(async (path: string) => {
    if (path === 'api/v1/graphs/g1/tables') {
      return [
        { id: 'mq1', title: 'title one', kind: 'parameterised', graph: 'g1', columns: ['gene_id', 'gene_label'], num_rows: 1200 },
        { id: 'mq2', title: 'title two', kind: 'standalone', graph: 'g1', columns: ['message'], num_rows: 1 },
      ]
    }
    if (path === 'api/v1/graphs') return ['g1']
    if (path === 'api/v1/stats') return {}
    throw new Error('unexpected GET ' + path)
  })
})

describe('EbiTablesHomePage', () => {
  it('shows progress until the tables load, then lists them with their files', async () => {
    renderPage()
    expect(screen.getByRole('progressbar')).toBeInTheDocument()

    expect(await screen.findByText('mq1')).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'Materialised Result Tables' })).toBeInTheDocument()
    expect(mockedGet).toHaveBeenCalledWith('api/v1/graphs/g1/tables')
    const row = screen.getByText('mq1').closest('tr')!
    expect(row).toHaveTextContent('title one')
    expect(row).toHaveTextContent('1,200')
    expect(within(row).getByRole('link', { name: /CSV/ })).toHaveAttribute('href', 'https://ftp.ebi.ac.uk/pub/databases/spot/kg/latest/query_results/g1/mq1.csv.gz')
    expect(within(row).getByRole('link', { name: /Parquet/ })).toHaveAttribute('href', 'https://ftp.ebi.ac.uk/pub/databases/spot/kg/latest/query_results/g1/mq1.parquet')
  })

  it('says where the tables are published and how to query one in place', async () => {
    renderPage()
    await screen.findByText('mq1')
    expect(screen.getByRole('link', { name: 'query_results/g1/' })).toHaveAttribute('href', 'https://ftp.ebi.ac.uk/pub/databases/spot/kg/latest/query_results/g1/')
    expect(screen.getByText(/SELECT \* FROM/)).toHaveTextContent("'https://ftp.ebi.ac.uk/pub/databases/spot/kg/latest/query_results/g1/disease_to_genes.parquet'")
  })

  it('opens the query a table is the results of, or the table itself', async () => {
    renderPage()
    fireEvent.click(await screen.findByText('mq1'))
    expect(screen.getByTestId('location')).toHaveTextContent('/graphs/g1/queries/mq1')
  })

  it('shows breadcrumbs for the graph', async () => {
    renderPage()
    const crumbs = within(screen.getByLabelText('breadcrumb'))
    expect(crumbs.getAllByRole('link').map((a) => a.getAttribute('href'))).toEqual(['/', '/graphs', '/graphs/g1/tables'])
  })
})
