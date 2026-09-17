import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import EdgesList from './EdgesList'
import GraphNode from '../../model/GraphNode'
import encodeNodeId from '../../encodeNodeId'
import { Page } from '../../app/api'

const api = vi.hoisted(() => ({ getPaginated: vi.fn(), get: vi.fn() }))

vi.mock('../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, getPaginated: api.getPaginated, get: api.get }
})

const node = new GraphNode({ 'grebi:nodeId': 'n-psoriasis', 'grebi:name': ['psoriasis'], _refs: {} })

const edges = [
  {
    'grebi:edgeId': 'e1',
    'grebi:type': 'biolink:has_phenotype',
    'grebi:datasources': ['MONDO', 'HPO'],
    from: { 'grebi:nodeId': 'n-psoriasis', 'grebi:name': ['psoriasis'] },
    to: { 'grebi:nodeId': 'n-lesion', 'grebi:name': ['skin lesion'] },
    'gwas:pvalue': '1e-8',
  },
]
// facets come back as plain objects; empty breakdowns are not worth a column
const facets = { 'grebi:datasources': { MONDO: 1, HPO: 1 }, 'gwas:pvalue': { '1e-8': 1 }, 'gwas:empty': {} }

const lastPath = (): string => api.getPaginated.mock.calls.at(-1)![0]
const lastQuery = () => new URLSearchParams(lastPath().split('?')[1])

type Props = Parameters<typeof EdgesList>[0]

function renderList(props: Partial<Props> = {}) {
  return render(
    <MemoryRouter>
      <EdgesList graph="g" node={node} direction="incoming" {...props} />
    </MemoryRouter>
  )
}

let log: ReturnType<typeof vi.spyOn>
beforeEach(() => {
  // "refreshing ..." is logged on every fetch
  log = vi.spyOn(console, 'log').mockImplementation(() => {})
  api.getPaginated.mockReset()
  api.getPaginated.mockImplementation(async () => new Page(0, 1, 1, 1, edges, facets as any))
})
afterEach(() => log.mockRestore())

describe('EdgesList', () => {
  it('lists incoming edges with datasources, source node and type, sorted by type', async () => {
    renderList()
    expect(screen.getByText('Loading edges...')).toBeInTheDocument()

    const from = await screen.findByRole('link', { name: 'psoriasis' })
    expect(from).toHaveAttribute('href', `/graphs/g/nodes/${encodeNodeId('n-psoriasis')}`)
    expect(lastPath().startsWith(`api/v1/graphs/g/nodes/${encodeNodeId('n-psoriasis')}/incoming_edges?`)).toBe(true)
    expect(lastQuery().toString()).toBe('page=0&size=10&sortBy=grebi%3Atype&sortDir=asc')

    for (const header of ['Datasources', 'From Node', 'Edge Type']) {
      expect(screen.getByText(header)).toBeInTheDocument()
    }
    expect(screen.queryByText('To Node')).toBeNull()
    expect(screen.getByText('biolink:has_phenotype')).toBeInTheDocument()
    expect(screen.queryByText('Loading edges...')).toBeNull()
  })

  it('lists outgoing edges with the target node instead', async () => {
    renderList({ direction: 'outgoing' })
    const to = await screen.findByRole('link', { name: 'skin lesion' })
    expect(to).toHaveAttribute('href', `/graphs/g/nodes/${encodeNodeId('n-lesion')}`)
    expect(lastPath()).toContain('/outgoing_edges?')
    expect(screen.getByText('To Node')).toBeInTheDocument()
    expect(screen.queryByText('From Node')).toBeNull()
  })

  it('adds a column for each edge property the facets report, skipping empty ones', async () => {
    renderList()
    await screen.findByRole('link', { name: 'psoriasis' })
    expect(screen.getByText('gwas:pvalue')).toBeInTheDocument()
    expect(screen.queryByText('gwas:empty')).toBeNull()
    expect(screen.queryByText('grebi:datasources')).toBeNull()

    // the property column reads the edge's own properties
    // the first cell is the edge's info button
    const cells = screen.getByText('biolink:has_phenotype').closest('tr')!.querySelectorAll('td')
    expect(cells).toHaveLength(5)
    expect(cells[4].textContent).toBe('1e-8')
  })

  it('reports the loaded edges to the parent', async () => {
    const onEdgesLoaded = vi.fn()
    renderList({ onEdgesLoaded })
    await screen.findByRole('link', { name: 'psoriasis' })
    expect(onEdgesLoaded).toHaveBeenCalledTimes(1)
    expect(onEdgesLoaded).toHaveBeenCalledWith(
      expect.objectContaining({ total: 1, datasources: ['MONDO', 'HPO'], propertyColumns: ['gwas:pvalue'] })
    )
  })

  it('unticking a datasource excludes it from the query', async () => {
    renderList()
    await screen.findByRole('link', { name: 'psoriasis' })
    // the selector lists datasources alphabetically: HPO, MONDO
    const boxes = screen.getAllByRole('checkbox')
    expect(boxes).toHaveLength(2)
    fireEvent.click(boxes[0])
    await waitFor(() => expect(api.getPaginated).toHaveBeenCalledTimes(2))
    expect(lastQuery().getAll('-grebi:datasources')).toEqual(['HPO'])

    fireEvent.click(screen.getByRole('button', { name: 'None' }))
    await waitFor(() => expect(api.getPaginated).toHaveBeenCalledTimes(3))
    expect(lastQuery().getAll('-grebi:datasources').sort()).toEqual(['HPO', 'MONDO'])
  })

  it('filters, sorts and passes extra params through to the query', async () => {
    renderList({ extraSearchParams: [['grebi:type', 'biolink:has_phenotype']] })
    await screen.findByRole('link', { name: 'psoriasis' })
    expect(lastQuery().get('grebi:type')).toBe('biolink:has_phenotype')

    fireEvent.change(screen.getByPlaceholderText('Search...'), { target: { value: 'lesion' } })
    await waitFor(() => expect(lastQuery().get('q')).toBe('lesion'))

    // the swap icons belong to Datasources and From Node; Edge Type is already the sort column
    fireEvent.click(screen.getAllByTestId('SwapVertIcon')[1])
    await waitFor(() => expect(lastQuery().get('sortBy')).toBe('grebi:from'))
    expect(lastQuery().get('sortDir')).toBe('asc')
    fireEvent.click(screen.getByTestId('ArrowDownwardIcon'))
    await waitFor(() => expect(lastQuery().get('sortDir')).toBe('desc'))
    expect(lastQuery().get('grebi:type')).toBe('biolink:has_phenotype')
  })
})

describe('EdgesList errors', () => {
  it('shows why the edges could not be loaded instead of the overlay', async () => {
    const { ApiError } = await import('../../app/api')
    api.getPaginated.mockRejectedValueOnce(new ApiError(502, 'u', '502 Bad Gateway'))
    renderList()
    const alert = await screen.findByRole('alert')
    expect(alert).toHaveTextContent('The edges could not be loaded')
    expect(alert).toHaveTextContent('502 Bad Gateway (HTTP 502)')
    expect(screen.queryByText('Loading edges...')).toBeNull()
  })
})

describe('EdgesList edge details', () => {
  it('opens the properties of an edge from its row', async () => {
    api.get.mockResolvedValue({ 'grebi:edgeId': edges[0]['grebi:edgeId'], 'grebi:type': 'is_a', 'grebi:datasources': ['MONDO'], 'source': ['a paper'], _refs: {} })
    renderList()
    const button = await screen.findByRole('button', { name: `View edge ${edges[0]['grebi:edgeId']}` })
    fireEvent.click(button)
    expect(await screen.findByText('Edge Properties')).toBeInTheDocument()
    expect(api.get).toHaveBeenCalledWith(`api/v1/graphs/g/edges/${encodeNodeId(edges[0]['grebi:edgeId'])}`)
    expect(await screen.findByText('a paper')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: /Open edge page/ })).toHaveAttribute('href', `/graphs/g/edges/${encodeNodeId(edges[0]['grebi:edgeId'])}`)
  })
})

describe('EdgesList export', () => {
  it('offers the whole list as CSV from the API, narrowed as the table is', async () => {
    renderList()
    const link = await screen.findByRole('link', { name: 'Download as CSV' })
    const url = new URL(link.getAttribute('href')!)
    expect(url.pathname).toBe(`/api/v1/graphs/g/nodes/${encodeNodeId('n-psoriasis')}/incoming_edges.csv`)
    expect(url.searchParams.get('sortBy')).toBe('grebi:type')
    expect(url.searchParams.get('page')).toBeNull()
  })
})
