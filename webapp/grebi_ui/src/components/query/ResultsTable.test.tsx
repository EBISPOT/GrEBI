import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import ResultsTable from './ResultsTable'

const rows = [
  {
    study: { 'grebi:nodeId': 'GCST010178', 'grebi:name': ['Shared mechanisms'], 'grebi:type': ['gwas:Study'] },
    trait: { 'grebi:nodeId': 'mondo:0005133', 'grebi:name': ['endometriosis'], 'grebi:type': ['ols:Class'] },
    pubmed_id: '32121467',
    reported_trait: 'Endometriosis or migraine',
    chromosome: null,
    edge_id: 'edge-1',
  },
]

vi.mock('../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return {
    ...mod,
    getPaginated: vi.fn(async () => new mod.Page(0, rows.length, 1, rows.length, rows, new Map())),
  }
})

const columns = [
  { column_id: 'study', column_type: 'GraphNodeId' },
  { column_id: 'trait', column_type: 'GraphNodeId' },
  { column_id: 'pubmed_id', column_type: 'PubmedId' },
  { column_id: 'reported_trait', column_type: 'string' },
  { column_id: 'chromosome', column_type: 'string' },
  { column_id: 'edge_id', column_type: 'EdgeId' },
]

function renderTable(resultColumns = columns) {
  return render(
    <MemoryRouter>
      <ResultsTable graph="test_gwas" queryId="q" params={{ trait_id: 'mondo:0005133' }} resultColumns={resultColumns} />
    </MemoryRouter>
  )
}

describe('ResultsTable', () => {
  it('renders each column type: node links, pubmed links, text, missing values, edge buttons', async () => {
    renderTable()
    const study = await screen.findByRole('link', { name: 'Shared mechanisms' })
    expect(study).toHaveAttribute('href', expect.stringContaining('/graphs/test_gwas/nodes/'))
    const pubmed = screen.getByRole('link', { name: '32121467' })
    expect(pubmed).toHaveAttribute('href', 'https://pubmed.ncbi.nlm.nih.gov/32121467/')
    expect(screen.getByText('Endometriosis or migraine')).toBeInTheDocument()
    // a null string column shows a dash
    expect(screen.getAllByText('-').length).toBeGreaterThan(0)
    // one edge-properties button per row
    expect(screen.getAllByTitle('View edge properties')).toHaveLength(1)
  })

  it('works for templates without an edge id column', async () => {
    renderTable(columns.filter(c => c.column_type !== 'EdgeId'))
    await screen.findByRole('link', { name: 'Shared mechanisms' })
    expect(screen.queryByTitle('View edge properties')).toBeNull()
    expect(screen.getByText('Endometriosis or migraine')).toBeInTheDocument()
  })
})
