import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import MaterialisedQueryTable from './MaterialisedQueryTable'

const api = vi.hoisted(() => ({ get: vi.fn() }))

vi.mock('../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, get: api.get }
})

const tables = [
  { id: 'gwas_by_disease', graph: 'g', title: 'GWAS associations of a disease', kind: 'parameterised',
    columns: ['disease_id', 'disease_label', 'snp_id', 'snp_label', 'p_value'], num_rows: 3812385 },
  { id: 'impc_x_gwas', graph: 'other', title: 'IMPC x GWAS', kind: 'standalone',
    columns: ['gwas_variant', 'mouse_gene_id'] },
]

function renderTable(graph?: string) {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <MaterialisedQueryTable graph={graph} />
    </MemoryRouter>
  )
}

function rowOf(id: string) {
  return screen.getByText(id).closest('tr')!
}

beforeEach(() => {
  api.get.mockReset()
  api.get.mockResolvedValue(tables)
})

describe('MaterialisedQueryTable', () => {
  it('shows a spinner until the tables arrive, then a row per table', async () => {
    renderTable('g')
    expect(screen.getByRole('progressbar')).toBeInTheDocument()

    expect(await screen.findByText('gwas_by_disease')).toBeInTheDocument()
    expect(screen.queryByRole('progressbar')).toBeNull()
    expect(api.get).toHaveBeenCalledTimes(1)
    expect(api.get).toHaveBeenCalledWith('api/v1/graphs/g/tables')
    for (const header of ['Table', 'Rows', 'Columns', 'Download']) {
      expect(screen.getByText(header)).toBeInTheDocument()
    }
    const row = rowOf('gwas_by_disease')
    expect(row).toHaveTextContent('GWAS associations of a disease')
    expect(row).toHaveTextContent('3,812,385')
    expect(row).toHaveTextContent('disease_id, disease_label, snp_id, snp_label, p_value')
  })

  it('links each table to its CSV and its Parquet file in the folder of its graph', async () => {
    renderTable('g')
    await screen.findByText('gwas_by_disease')

    const mine = within(rowOf('gwas_by_disease'))
    expect(mine.getByRole('link', { name: /CSV/ })).toHaveAttribute('href', 'https://ftp.ebi.ac.uk/pub/databases/spot/kg/latest/query_results/g/gwas_by_disease.csv.gz')
    expect(mine.getByRole('link', { name: /Parquet/ })).toHaveAttribute('href', 'https://ftp.ebi.ac.uk/pub/databases/spot/kg/latest/query_results/g/gwas_by_disease.parquet')
    expect(mine.getByRole('link', { name: /CSV/ })).toHaveAttribute('target', '_blank')

    const other = within(rowOf('impc_x_gwas'))
    expect(other.getByRole('link', { name: /Parquet/ })).toHaveAttribute('href', 'https://ftp.ebi.ac.uk/pub/databases/spot/kg/latest/query_results/other/impc_x_gwas.parquet')
  })

  it('names a table by a link to the query it is the results of, or to its own page', async () => {
    renderTable('g')
    await screen.findByText('gwas_by_disease')
    expect(screen.getByText('gwas_by_disease').closest('a')).toHaveAttribute('href', '/graphs/g/queries/gwas_by_disease')
    expect(screen.getByText('impc_x_gwas').closest('a')).toHaveAttribute('href', '/graphs/other/tables/impc_x_gwas')
  })

  it('a table that could not be counted has no row count', async () => {
    renderTable('g')
    await screen.findByText('impc_x_gwas')
    const cells = within(rowOf('impc_x_gwas')).getAllByRole('cell')
    expect(cells[1].textContent).toBe('(no data)')
  })

  it('filters the rows locally through the search box', async () => {
    renderTable('g')
    await screen.findByText('impc_x_gwas')
    fireEvent.change(screen.getByPlaceholderText('Search...'), { target: { value: 'impc' } })
    expect(screen.getByText('impc_x_gwas')).toBeInTheDocument()
    expect(screen.queryByText('gwas_by_disease')).toBeNull()
    expect(api.get).toHaveBeenCalledTimes(1)
  })

  it('without a graph it lists the tables of every graph', async () => {
    renderTable(undefined)
    expect(await screen.findByText('gwas_by_disease')).toBeInTheDocument()
    expect(api.get).toHaveBeenCalledTimes(1)
    expect(api.get).toHaveBeenCalledWith('api/v1/tables')
  })

  it('says so when the tables cannot be loaded', async () => {
    api.get.mockRejectedValue(new Error('boom'))
    renderTable('g')
    expect(await screen.findByRole('alert')).toHaveTextContent('The tables could not be loaded')
  })
})
