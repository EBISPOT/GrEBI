import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, within } from '@testing-library/react'
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom'
import EbiQueriesHomePage from './EbiQueriesHomePage'

vi.mock('../../../app/api', async (importOriginal) => ({ ...(await importOriginal<any>()), get: vi.fn(), getPaginated: vi.fn(), post: vi.fn() }))
import { get } from '../../../app/api'
const mockedGet = vi.mocked(get)

const topics = [
  { id: 'gwas_catalog', name: 'GWAS Catalog', type: 'Datasource', description: '', url: '' },
  { id: 'disease', name: 'Disease', type: 'Domain', description: '', url: '' },
]
// single SourceId parameter with a titled example: the read-only question needs no API call
const t1 = {
  id: 'gwas_by_trait', title: 'GWAS by trait', question: 'Which [studies]{study} report {trait_id}?',
  topics: ['gwas_catalog'], graphs: ['g1'],
  params: [{ param_id: 'trait_id', param_name: 'Trait', param_type: 'SourceId' }],
  result_columns: [{ column_id: 'study', column_type: 'GraphNodeId' }],
  examples: [{ title: 'psoriasis', params: { trait_id: 'mondo:0005083' } }],
}
const t2 = {
  id: 'genes_for_disease', title: 'Genes for disease', question: 'Which [genes]{gene} are linked to {disease_id}?',
  topics: ['disease'], graphs: ['g1'],
  params: [{ param_id: 'disease_id', param_name: 'Disease', param_type: 'SourceId' }],
  result_columns: [{ column_id: 'gene', column_type: 'GraphNodeId' }],
  examples: [{ title: 'asthma', params: { disease_id: 'mondo:0004979' } }],
}

function LocationDisplay() {
  const loc = useLocation()
  return <div data-testid="location">{loc.pathname + loc.search}</div>
}

function renderPage(initialTopic?: string) {
  return render(
    <MemoryRouter initialEntries={['/graphs/g1/queries']}>
      <Routes>
        <Route path="/graphs/:graph/queries" element={<><EbiQueriesHomePage initialTopic={initialTopic} /><LocationDisplay /></>} />
        <Route path="*" element={<LocationDisplay />} />
      </Routes>
    </MemoryRouter>
  )
}

const rowIds = () => screen.getAllByRole('row').slice(1).map((r) => r.querySelector('td')?.textContent)

beforeEach(() => {
  mockedGet.mockReset()
  mockedGet.mockImplementation(async (path: string) => {
    if (path === 'api/v1/topics') return topics
    if (path === 'api/v1/graphs/g1/query_templates') return [t1, t2]
    if (path === 'api/v1/graphs') return ['g1']
    if (path === 'api/v1/stats') return {}
    throw new Error('unexpected GET ' + path)
  })
})

describe('EbiQueriesHomePage', () => {
  it('shows progress until topics and templates load, then lists every template with its example', async () => {
    renderPage()
    expect(screen.getAllByRole('progressbar').length).toBeGreaterThan(0)

    expect(await screen.findByText('gwas_by_trait')).toBeInTheDocument()
    expect(rowIds()).toEqual(['gwas_by_trait', 'genes_for_disease'])
    const row = screen.getByText('gwas_by_trait').closest('tr')!
    expect(row.textContent).toContain('trait_id')
    expect(row.textContent).toContain('study')
    expect(row.textContent).toContain('Which studies report psoriasis?')
    expect(screen.getByRole('link', { name: /Learn more about GrEBI queries/ })).toHaveAttribute('href', '/docs/queries')
  })

  it('pre-ticks the initial topic and only lists matching templates', async () => {
    renderPage('disease')
    await screen.findByText('genes_for_disease')
    expect(rowIds()).toEqual(['genes_for_disease'])
    expect(screen.getByLabelText('Disease')).toBeChecked()
    expect(screen.getByLabelText('GWAS Catalog')).not.toBeChecked()
  })

  it('filters by topic from the facets and can clear the filters again', async () => {
    renderPage()
    await screen.findByText('gwas_by_trait')

    fireEvent.click(screen.getByLabelText('GWAS Catalog'))
    expect(rowIds()).toEqual(['gwas_by_trait'])
    // a second topic widens the selection
    fireEvent.click(screen.getByLabelText('Disease'))
    expect(rowIds()).toEqual(['gwas_by_trait', 'genes_for_disease'])

    fireEvent.click(screen.getByRole('button', { name: 'Clear all' }))
    expect(screen.getByLabelText('GWAS Catalog')).not.toBeChecked()
    expect(rowIds()).toEqual(['gwas_by_trait', 'genes_for_disease'])
  })

  it('says so when the input and output filters exclude everything', async () => {
    renderPage()
    await screen.findByText('gwas_by_trait')
    const facets = within(screen.getByText('Filters').parentElement!.parentElement!)
    fireEvent.click(facets.getByLabelText('trait_id'))
    fireEvent.click(facets.getByLabelText('gene'))
    expect(screen.getByText('No queries match the current filters.')).toBeInTheDocument()
  })

  it('clicking a template opens it with its first example filled in', async () => {
    renderPage()
    fireEvent.click(await screen.findByText('genes_for_disease'))
    expect(screen.getByTestId('location')).toHaveTextContent('/graphs/g1/queries/genes_for_disease?disease_id=mondo%3A0004979')
  })
})

describe('EbiQueriesHomePage errors', () => {
  it('shows why the queries could not be listed', async () => {
    const { ApiError } = await import('../../../app/api')
    mockedGet.mockImplementation(async (path: string) => {
      if (path === 'api/v1/topics') return topics
      if (path === 'api/v1/graphs/g1/query_templates') throw new ApiError(500, path, 'templates unreadable')
      if (path === 'api/v1/graphs') return ['g1']
      if (path === 'api/v1/stats') return {}
      throw new Error('unexpected GET ' + path)
    })
    renderPage()
    const alert = await screen.findByRole('alert')
    expect(alert).toHaveTextContent('The queries could not be loaded')
    expect(alert).toHaveTextContent('templates unreadable (HTTP 500)')
    expect(screen.queryByRole('progressbar')).toBeNull()
  })
})
