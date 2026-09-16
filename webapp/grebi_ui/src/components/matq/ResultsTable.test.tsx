import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import ResultsTable from './ResultsTable'
import encodeNodeId from '../../encodeNodeId'
import { Page } from '../../app/api'

const api = vi.hoisted(() => ({ getPaginated: vi.fn() }))

vi.mock('../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, getPaginated: api.getPaginated }
})

// A materialised row: bare ids resolved through the row's own _refs, plus
// bookkeeping columns the table hides.
const rows = [
  {
    id: 'r1',
    graph: 'other',
    disease: 'mondo:0005083',
    gene: 'hgnc:1101',
    pvalue: '1e-8',
    _node_ids: ['n1', 'n2'],
    _version_: 1,
    _refs: {
      'mondo:0005083': { 'grebi:nodeId': 'n1', 'grebi:name': ['psoriasis'] },
      'hgnc:1101': { 'grebi:nodeId': 'n2', 'grebi:name': ['BRCA2'] },
    },
  },
]

const lastPath = (): string => api.getPaginated.mock.calls.at(-1)![0]

function renderTable(extraSearchParams?: string[][]) {
  return render(
    <MemoryRouter>
      <ResultsTable graph="g" queryid="gwas_by_disease" extraSearchParams={extraSearchParams} />
    </MemoryRouter>
  )
}

let dir: ReturnType<typeof vi.spyOn>
beforeEach(() => {
  // every cell is console.dir'd
  dir = vi.spyOn(console, 'dir').mockImplementation(() => {})
  api.getPaginated.mockReset()
  api.getPaginated.mockImplementation(async () => new Page(0, rows.length, 1, rows.length, rows, new Map()))
})
afterEach(() => dir.mockRestore())

describe('materialised ResultsTable', () => {
  it('shows a loading overlay until the first page arrives, then a column per data key except hidden ones', async () => {
    renderTable()
    expect(screen.getByText('Loading results...')).toBeInTheDocument()

    const disease = await screen.findByRole('link', { name: 'psoriasis' })
    expect(screen.queryByText('Loading results...')).toBeNull()
    for (const header of ['graph', 'disease', 'gene', 'pvalue']) {
      expect(screen.getByText(header)).toBeInTheDocument()
    }
    for (const hidden of ['id', '_refs', '_node_ids', '_version_']) {
      expect(screen.queryByText(hidden)).toBeNull()
    }
    // ids resolved through _refs link into the row's own graph
    expect(disease).toHaveAttribute('href', `/graphs/other/nodes/${encodeNodeId('mondo:0005083')}`)
    expect(screen.getByRole('link', { name: 'BRCA2' })).toBeInTheDocument()
    expect(screen.getByText(/1e-8/)).toBeInTheDocument()
  })

  it('requests the first page with default paging and any extra params', async () => {
    renderTable([['subgraph', 'gwas']])
    await screen.findByRole('link', { name: 'psoriasis' })
    expect(api.getPaginated).toHaveBeenCalledTimes(1)
    expect(lastPath()).toBe('api/v1/graphs/g/materialised_queries/gwas_by_disease?page=0&size=10&subgraph=gwas')
  })

  it('typing in the search box re-queries with q', async () => {
    renderTable()
    await screen.findByRole('link', { name: 'psoriasis' })
    fireEvent.change(screen.getByPlaceholderText('Search...'), { target: { value: 'brca' } })
    await waitFor(() => expect(api.getPaginated).toHaveBeenCalledTimes(2))
    expect(lastPath()).toBe('api/v1/graphs/g/materialised_queries/gwas_by_disease?page=0&size=10&q=brca')
  })

  it('sorting a column re-queries with sortBy and sortDir', async () => {
    renderTable()
    await screen.findByRole('link', { name: 'psoriasis' })
    fireEvent.click(screen.getAllByTestId('SwapVertIcon')[1])
    await waitFor(() => expect(api.getPaginated).toHaveBeenCalledTimes(2))
    expect(lastPath()).toContain('sortBy=')
    expect(lastPath()).toContain('sortDir=asc')
  })

  it('changing rows per page re-queries with the new size', async () => {
    renderTable()
    await screen.findByRole('link', { name: 'psoriasis' })
    fireEvent.change(screen.getByDisplayValue('10'), { target: { value: '25' } })
    await waitFor(() => expect(lastPath()).toContain('size=25'))
  })
})
