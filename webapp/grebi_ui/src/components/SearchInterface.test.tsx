import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import SearchInterface from './SearchInterface'
import encodeNodeId from '../encodeNodeId'
import { Page } from '../app/api'

const api = vi.hoisted(() => ({ get: vi.fn(), getPaginated: vi.fn() }))

vi.mock('../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, get: api.get, getPaginated: api.getPaginated }
})

const hits = [
  {
    'grebi:nodeId': 'n1',
    'grebi:name': ['psoriasis'],
    'grebi:type': ['biolink:Disease'],
    'grebi:datasources': ['MONDO', 'GWAS'],
    'grebi:sourceIds': ['mondo:0005083', 'efo:0000676'],
    'grebi:description': ['A chronic skin disease'],
  },
  {
    'grebi:nodeId': 'n2',
    'grebi:name': ['psoriatic arthritis'],
    'grebi:type': ['biolink:Disease'],
    'grebi:datasources': ['MONDO'],
    'grebi:sourceIds': ['mondo:0011849'],
  },
]
const facets = {
  'grebi:datasources': { GWAS: 3, MONDO: 5, 'OLS.efo': 2 },
  'grebi:type': { 'biolink:Disease': 4, entity: 9, 'ols:Class': 0 },
}

// SearchInterface asks for `api/v1/graphs/g/search` with a URLSearchParams; the
// SearchBox it embeds asks for its own suggestions with the query in the path.
const searchCalls = () => api.getPaginated.mock.calls.filter((c) => c[0] === 'api/v1/graphs/g/search')
const lastSearch = (): URLSearchParams => searchCalls().at(-1)![1]

function renderSearch(search: string) {
  return render(
    <MemoryRouter initialEntries={[`/graphs/g/search${search}`]}>
      <SearchInterface graph="g" />
    </MemoryRouter>
  )
}

beforeEach(() => {
  api.get.mockReset()
  api.getPaginated.mockReset()
  api.get.mockImplementation(async (path: string) => {
    if (path.includes('/embedding_models')) return []
    if (path.includes('/suggest?')) return []
    // n=10 is the results page; the search box asks for n=5
    if (path.includes('/semantic_search?')) return path.includes('n=10') ? [{ ...hits[0], 'grebi:searchScore': 0.925 }] : []
    throw new Error('unexpected request ' + path)
  })
  api.getPaginated.mockImplementation(async (path: string) =>
    path === 'api/v1/graphs/g/search'
      ? new Page(0, hits.length, 1, hits.length, hits, facets as any)
      : new Page(0, 0, 0, 0, [], new Map())
  )
})

describe('SearchInterface', () => {
  it('runs a faceted lexical search for q and lists the hits with their details', async () => {
    renderSearch('?q=psoriasis')
    expect(screen.getByText('Search results for: psoriasis')).toBeInTheDocument()

    const link = await screen.findByRole('link', { name: 'psoriasis' })
    expect(link).toHaveAttribute('href', `/graphs/g/nodes/${encodeNodeId('n1')}`)
    // an exact name match is bold
    expect(link).toHaveClass('font-bold')
    expect(screen.getByRole('link', { name: 'psoriatic arthritis' })).not.toHaveClass('font-bold')
    expect(screen.getAllByText('Disease')).toHaveLength(2)
    expect(screen.getByText('mondo:0005083')).toBeInTheDocument()
    expect(screen.getByText('A chronic skin disease')).toBeInTheDocument()

    expect(searchCalls()).toHaveLength(1)
    const p = lastSearch()
    expect(p.get('q')).toBe('psoriasis')
    expect(p.get('page')).toBe('0')
    expect(p.get('size')).toBe('10')
    expect(p.getAll('facet')).toEqual(['grebi:datasources', 'grebi:type'])
    expect(screen.queryByText('Search results loading...')).toBeNull()
  })

  it('shows type and datasource facets with counts, hiding the entity type and zero counts', async () => {
    renderSearch('?q=psoriasis')
    await screen.findByRole('link', { name: 'psoriasis' })
    expect(screen.getByLabelText('biolink:Disease (4)')).not.toBeChecked()
    expect(screen.queryByText(/entity/)).toBeNull()
    expect(screen.queryByText(/ols:Class/)).toBeNull()
    // datasources are listed by descending count
    const datasources = screen.getAllByText(/^(GWAS|MONDO|OLS\.efo) \(\d+\)$/).map((e) => e.textContent)
    expect(datasources).toEqual(['MONDO (5)', 'GWAS (3)', 'OLS.efo (2)'])
  })

  it('ticking facets re-runs the search filtered by them, from the first page', async () => {
    renderSearch('?q=psoriasis')
    await screen.findByRole('link', { name: 'psoriasis' })

    fireEvent.click(screen.getByLabelText('biolink:Disease (4)'))
    await waitFor(() => expect(searchCalls()).toHaveLength(2))
    expect(lastSearch().getAll('grebi:type')).toEqual(['biolink:Disease'])
    expect(lastSearch().get('page')).toBe('0')
    expect(screen.getByLabelText('biolink:Disease (4)')).toBeChecked()

    fireEvent.click(screen.getByLabelText('MONDO (5)'))
    await waitFor(() => expect(searchCalls()).toHaveLength(3))
    expect(lastSearch().getAll('grebi:datasources')).toEqual(['MONDO'])
    expect(lastSearch().getAll('grebi:type')).toEqual(['biolink:Disease'])
  })

  it('the datasource box narrows the facet list and can be cleared', async () => {
    renderSearch('?q=psoriasis')
    await screen.findByRole('link', { name: 'psoriasis' })
    const box = screen.getByPlaceholderText('Filter datasources...')
    fireEvent.change(box, { target: { value: 'gw' } })
    expect(screen.getByText('GWAS (3)')).toBeInTheDocument()
    expect(screen.queryByText('MONDO (5)')).toBeNull()

    fireEvent.click(box.parentElement!.querySelector('button')!)
    expect(screen.getByText('MONDO (5)')).toBeInTheDocument()
    expect(box).toHaveValue('')
  })

  it('load more fetches the next page and appends it', async () => {
    api.getPaginated.mockImplementation(async (path: string, params?: URLSearchParams) => {
      if (path !== 'api/v1/graphs/g/search') return new Page(0, 0, 0, 0, [], new Map())
      const page = Number(params!.get('page'))
      const rows = page === 0 ? hits : [{ 'grebi:nodeId': 'n3', 'grebi:name': ['plaque psoriasis'], 'grebi:sourceIds': [] }]
      return new Page<any>(page, rows.length, 2, 3, rows, facets as any)
    })
    renderSearch('?q=psoriasis')
    await screen.findByRole('link', { name: 'psoriasis' })

    fireEvent.click(screen.getByRole('button', { name: 'Load more results...' }))
    expect(await screen.findByRole('link', { name: 'plaque psoriasis' })).toBeInTheDocument()
    expect(lastSearch().get('page')).toBe('1')
    expect(screen.getByRole('link', { name: 'psoriasis' })).toBeInTheDocument()
    // everything is now shown
    expect(screen.queryByRole('button', { name: 'Load more results...' })).toBeNull()
  })

  it('a model parameter switches to semantic search with scores and no facets', async () => {
    renderSearch('?q=psoriasis&model=minilm')
    expect(await screen.findByRole('link', { name: 'psoriasis' })).toBeInTheDocument()
    expect(screen.getByText('92.5%')).toBeInTheDocument()
    expect(api.get).toHaveBeenCalledWith('api/v1/graphs/g/semantic_search?q=psoriasis&model=minilm&n=10&resolve=true')
    expect(searchCalls()).toHaveLength(0)
    expect(screen.queryByText('Filter results')).toBeNull()
  })

  it('changing rows per page restarts from the first page with the new size', async () => {
    renderSearch('?q=psoriasis')
    await screen.findByRole('link', { name: 'psoriasis' })
    fireEvent.change(screen.getByDisplayValue('10'), { target: { value: '25' } })
    await waitFor(() => expect(searchCalls()).toHaveLength(2))
    expect(lastSearch().get('size')).toBe('25')
    expect(lastSearch().get('page')).toBe('0')
  })
})

describe('SearchInterface errors', () => {
  it('shows why the search failed and stops waiting', async () => {
    const { ApiError } = await import('../app/api')
    api.getPaginated.mockImplementation(async (path: string) => {
      if (path === 'api/v1/graphs/g/search') throw new ApiError(503, path, 'search index down')
      return new Page<any>(0, 0, 0, 0, [], new Map())
    })
    api.get.mockResolvedValue([])
    renderSearch('?q=psoriasis')
    const alert = await screen.findByRole('alert')
    expect(alert).toHaveTextContent('Search results could not be loaded')
    expect(alert).toHaveTextContent('search index down (HTTP 503)')
    await waitFor(() => expect(screen.queryByText('Search results loading...')).toBeNull())
  })
})
