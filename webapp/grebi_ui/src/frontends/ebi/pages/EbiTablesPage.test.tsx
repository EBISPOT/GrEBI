import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import EbiTablesPage from './EbiTablesPage'

vi.mock('../../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, get: vi.fn(async () => []), getPaginated: vi.fn(), post: vi.fn() }
})
import { getPaginated, Page } from '../../../app/api'
const mockedGetPaginated = vi.mocked(getPaginated)

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/graphs/g1/tables/mq1']}>
      <Routes>
        <Route path="/graphs/:graph/tables/:queryid" element={<EbiTablesPage />} />
      </Routes>
    </MemoryRouter>
  )
}

beforeEach(() => {
  mockedGetPaginated.mockReset()
  mockedGetPaginated.mockResolvedValue(
    new Page<any>(0, 1, 1, 1, [{ gene: 'BRCA1', disease: 'psoriasis', _refs: {} }], new Map())
  )
})

describe('EbiTablesPage', () => {
  it('names the query and shows breadcrumbs down to it', () => {
    renderPage()
    expect(screen.getByRole('heading', { name: 'mq1' })).toBeInTheDocument()
    const crumbs = within(screen.getByLabelText('breadcrumb'))
    expect(crumbs.getAllByRole('link').map((a) => a.getAttribute('href'))).toEqual(['/', '/graphs', '/graphs/g1/tables', '/graphs/g1/tables/mq1'])
  })

  it('loads the first page of results and shows a column per field', async () => {
    renderPage()
    expect(screen.getByText('Loading results...')).toBeInTheDocument()

    expect(await screen.findByText('BRCA1')).toBeInTheDocument()
    expect(screen.getByText('psoriasis')).toBeInTheDocument()
    expect(screen.getByText('gene')).toBeInTheDocument()
    expect(screen.getByText('disease')).toBeInTheDocument()
    expect(screen.queryByText('_refs')).toBeNull()
    expect(mockedGetPaginated).toHaveBeenCalledWith('api/v1/graphs/g1/materialised_queries/mq1?page=0&size=10')
  })
})

describe('EbiTablesPage download', () => {
  it('links to the whole table as CSV on the FTP', () => {
    renderPage()
    expect(screen.getByRole('link', { name: /Download the whole table as CSV/ })).toHaveAttribute('href', expect.stringMatching(/\/latest\/query_results\/.+\.results\.csv\.gz$/))
  })
})
