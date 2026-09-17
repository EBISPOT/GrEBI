import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react'
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom'
import EbiQueryPage from './EbiQueryPage'

vi.mock('../../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, get: vi.fn(), getPaginated: vi.fn(), post: vi.fn() }
})
import { get, getPaginated, Page } from '../../../app/api'
const mockedGet = vi.mocked(get)
const mockedGetPaginated = vi.mocked(getPaginated)

// a string parameter keeps the inputs simple (no node autocomplete)
const template = {
  id: 'q1',
  title: 'Diseases associated with a gene',
  question: 'Which [diseases]{disease} are associated with gene {gene_symbol}?',
  description: '',
  graphs: ['g1'],
  topics: [],
  cypher_match_fragment: 'MATCH (g:Gene {symbol: $gene_symbol})-[:associated_with]->(d:Disease)',
  cypher_return_fragment: 'RETURN d AS disease',
  cypher_count_fragment: '',
  params: [{ param_id: 'gene_symbol', param_name: 'Gene symbol', param_type: 'string' }],
  result_columns: [{ column_id: 'disease', column_type: 'GraphNodeId' }],
  examples: [
    { title: 'BRCA1', params: { gene_symbol: 'BRCA1' } },
    { title: 'TP53', params: { gene_symbol: 'TP53' } },
  ],
}
let served: any = template

function LocationDisplay() {
  const loc = useLocation()
  return <div data-testid="location">{loc.pathname + loc.search}</div>
}

function renderPage(search = '') {
  return render(
    <MemoryRouter initialEntries={[`/graphs/g1/queries/q1${search}`]}>
      <Routes>
        <Route path="/graphs/:graph/queries/:queryid" element={<><EbiQueryPage /><LocationDisplay /></>} />
      </Routes>
    </MemoryRouter>
  )
}

beforeEach(() => {
  served = template
  mockedGet.mockReset()
  mockedGetPaginated.mockReset()
  mockedGet.mockImplementation(async (path: string) => {
    if (path === 'api/v1/graphs/g1/query_templates/q1') return served
    if (path === 'api/v1/graphs') return ['g1']
    if (path === 'api/v1/stats') return {}
    throw new Error('unexpected GET ' + path)
  })
  mockedGetPaginated.mockResolvedValue(new Page<any>(0, 0, 0, 0, [], new Map()))
})

describe('EbiQueryPage', () => {
  it('loads the template and renders its title and question with input/output badges', async () => {
    const { container } = renderPage()
    expect(container.querySelector('.spinner-default')).not.toBeNull()

    expect(await screen.findByRole('heading', { name: 'Diseases associated with a gene' })).toBeInTheDocument()
    expect(mockedGet).toHaveBeenCalledWith('api/v1/graphs/g1/query_templates/q1')
    // the examples repeat the wording in spans; the question itself is the paragraph
    const question = screen.getByText(/are associated with gene/, { selector: 'p' })
    expect(question.querySelector('strong')).toHaveTextContent('diseases')
    expect(within(question).getByText('disease')).toBeInTheDocument()
    expect(within(question).getByText('gene_symbol')).toBeInTheDocument()

    const crumbs = within(screen.getByLabelText('breadcrumb'))
    expect(crumbs.getAllByRole('link').map((a) => a.getAttribute('href'))).toEqual(['/', '/graphs', '/graphs/g1/queries', '/graphs/g1/queries/q1'])
    expect(crumbs.getByRole('link', { name: 'q1' }).querySelector('code')).not.toBeNull()
  })

  it('lists the examples and running one puts its parameters in the URL and fetches results', async () => {
    renderPage()
    const examples = (await screen.findByText('Examples')).parentElement!
    expect(examples.textContent).toContain('Which diseases are associated with gene BRCA1?')
    expect(examples.textContent).toContain('Which diseases are associated with gene TP53?')
    expect(mockedGetPaginated).not.toHaveBeenCalled()

    fireEvent.click(within(examples).getByText('TP53'))
    expect(screen.getByTestId('location')).toHaveTextContent('/graphs/g1/queries/q1?gene_symbol=TP53')
    await waitFor(() => expect(mockedGetPaginated).toHaveBeenCalled())
    expect(mockedGetPaginated.mock.calls[0][0]).toBe('api/v1/graphs/g1/query/q1')
    expect(screen.getByRole('heading', { name: 'Results' })).toBeInTheDocument()
  })

  it('runs straight away when the parameters are already in the URL', async () => {
    renderPage('?gene_symbol=BRCA1')
    await screen.findByRole('heading', { name: 'Results' })
    expect(mockedGetPaginated.mock.calls[0][0]).toBe('api/v1/graphs/g1/query/q1')
    expect(screen.getByDisplayValue('BRCA1')).toBeInTheDocument()
  })

  it('has no examples sidebar for templates without examples', async () => {
    served = { ...template, examples: [] }
    renderPage()
    await screen.findByRole('heading', { name: 'Diseases associated with a gene' })
    expect(screen.queryByText('Examples')).toBeNull()
  })

  it('shows the generated code and the Cypher query in tabs', async () => {
    renderPage()
    await screen.findByRole('heading', { name: 'Diseases associated with a gene' })
    for (const tab of ['cURL', 'Python', 'R', 'Cypher Query']) {
      expect(screen.getByRole('button', { name: tab })).toBeInTheDocument()
    }
    fireEvent.click(screen.getByRole('button', { name: 'Cypher Query' }))
    expect(document.querySelector('pre.bg-slate-900')!.textContent).toContain('MATCH (g:Gene {symbol: $gene_symbol})')
  })
})

describe('EbiQueryPage errors', () => {
  it('says when there is no such query template', async () => {
    const { ApiError } = await import('../../../app/api')
    mockedGet.mockImplementation(async (path: string) => {
      if (path === 'api/v1/graphs/g1/query_templates/q1') throw new ApiError(404, path, 'No such template')
      if (path === 'api/v1/graphs') return ['g1']
      if (path === 'api/v1/stats') return {}
      throw new Error('unexpected GET ' + path)
    })
    renderPage()
    const alert = await screen.findByRole('alert')
    expect(alert).toHaveTextContent('The query template not found')
    expect(alert).toHaveTextContent('No such template')
    expect(document.querySelector('.spinner-default')).toBeNull()
  })
})
