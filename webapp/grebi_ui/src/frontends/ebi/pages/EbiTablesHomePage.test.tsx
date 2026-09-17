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
    if (path === 'api/v1/graphs/g1/materialised_queries') {
      return [
        { id: 'mq1', description: 'desc one', end_time: '2025-01-01', graph: 'g1' },
        { id: 'mq2', description: 'desc two', end_time: '2025-02-02', graph: 'other_graph' },
      ]
    }
    if (path === 'api/v1/graphs/g1') return { materialised_queries: [] }
    if (path === 'api/v1/graphs') return ['g1']
    if (path === 'api/v1/stats') return {}
    throw new Error('unexpected GET ' + path)
  })
})

describe('EbiTablesHomePage', () => {
  it('shows progress until the materialised queries load, then lists them', async () => {
    renderPage()
    expect(screen.getByRole('progressbar')).toBeInTheDocument()

    expect(await screen.findByText('mq1')).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'Materialised Result Tables' })).toBeInTheDocument()
    expect(mockedGet).toHaveBeenCalledWith('api/v1/graphs/g1/materialised_queries')
    const row = screen.getByText('mq1').closest('tr')!
    expect(row).toHaveTextContent('desc one')
    expect(row).toHaveTextContent('2025-01-01')
    expect(within(row).getAllByRole('link')[0]).toHaveAttribute('href', 'https://github.com/EBISPOT/GrEBI/blob/dev/query_templates/mq1.yaml')
    expect(within(row).getByRole('link', { name: /CSV/ })).toHaveAttribute('href', 'https://ftp.ebi.ac.uk/pub/databases/spot/kg/latest/query_results/mq1.results.csv.gz')
  })

  it('opens a query\'s table for the graph it was materialised in', async () => {
    renderPage()
    fireEvent.click(await screen.findByText('desc two'))
    expect(screen.getByTestId('location')).toHaveTextContent('/graphs/other_graph/tables/mq2')
  })

  it('shows breadcrumbs for the graph', async () => {
    renderPage()
    const crumbs = within(screen.getByLabelText('breadcrumb'))
    expect(crumbs.getAllByRole('link').map((a) => a.getAttribute('href'))).toEqual(['/', '/graphs', '/graphs/g1/tables'])
  })
})
