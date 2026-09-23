import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import QueryInterface from './QueryInterface'
import { Page } from '../../app/api'

const api = vi.hoisted(() => ({ getPaginated: vi.fn() }))

vi.mock('../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, getPaginated: api.getPaginated }
})

const psoriasis = {
  'grebi:nodeId': 'n-psoriasis',
  'grebi:curie': 'mondo:0005083',
  'grebi:name': ['psoriasis'],
  'grebi:type': ['biolink:Disease'],
  'grebi:sourceIds': ['mondo:0005083'],
}
const resultRows = [{ study: { 'grebi:nodeId': 'GCST1', 'grebi:name': ['Study one'] } }]

const base: any = {
  id: 'gwas_studies_by_disease',
  title: 'GWAS studies by disease',
  question: 'What GWAS [studies]{study} report {disease_id}?',
  description: '',
  graphs: ['g'],
  topics: [],
  cypher_match_fragment: 'MATCH (study:Study)',
  cypher_return_fragment: 'RETURN study',
  cypher_count_fragment: '',
  params: [{ param_id: 'disease_id', param_name: 'Disease', param_type: 'SourceId', values_with_type: 'biolink:Disease' }],
  result_columns: [{ column_id: 'study', column_type: 'GraphNodeId' }],
  examples: [{ title: 'psoriasis', params: { disease_id: 'mondo:0005083' } }],
}
const textTemplate = {
  ...base,
  id: 'genes_on_chromosome',
  params: [
    { param_id: 'chrom', param_name: 'Chromosome', param_type: 'string' },
    { param_id: 'min_score', param_name: 'Minimum score', param_type: 'float', param_default: '0.5' },
  ],
}
const noParams = { ...base, id: 'all_studies', params: [] }

const queryCalls = () => api.getPaginated.mock.calls.filter((c) => String(c[0]).includes('/query/'))
const nodeCalls = () => api.getPaginated.mock.calls.filter((c) => String(c[0]).endsWith('/nodes'))

function renderInterface(template: any, search = '', sidebar?: React.ReactNode) {
  return render(
    <MemoryRouter initialEntries={[`/graphs/g/queries/${template.id}${search}`]}>
      <QueryInterface graph="g" queryTemplate={template} sidebar={sidebar} />
    </MemoryRouter>
  )
}

let log: ReturnType<typeof vi.spyOn>
beforeEach(() => {
  // "Submitting values:" is logged on every submit
  log = vi.spyOn(console, 'log').mockImplementation(() => {})
  api.getPaginated.mockReset()
  api.getPaginated.mockImplementation(async (path: string) =>
    path.endsWith('/nodes')
      ? new Page(0, 1, 1, 1, [psoriasis], new Map())
      : new Page(0, resultRows.length, 1, resultRows.length, resultRows, {} as any)
  )
})
afterEach(() => log.mockRestore())

describe('QueryInterface', () => {
  it('shows the code tabs and an input row per parameter, and waits before querying', () => {
    const { container } = renderInterface(base)
    for (const tab of ['cURL', 'Python', 'R', 'Cypher Query']) {
      expect(screen.getByRole('button', { name: tab })).toBeInTheDocument()
    }
    expect(container.querySelector('pre')!.textContent).toContain('/api/v1/graphs/g/query/gwas_studies_by_disease.csv')
    expect(screen.getByText('Inputs')).toBeInTheDocument()
    expect(screen.getByText('disease_id')).toBeInTheDocument()
    expect(screen.getByPlaceholderText('Type to search…')).toBeInTheDocument()
    expect(screen.queryByText('Results')).toBeNull()
    expect(api.getPaginated).not.toHaveBeenCalled()
  })

  it('resolves a SourceId from the query string, shows the node and runs the query with its curie', async () => {
    renderInterface(base, '?disease_id=mondo:0005083')
    await waitFor(() => expect(nodeCalls()).toHaveLength(1))
    expect(nodeCalls()[0]).toEqual(['api/v1/graphs/g/nodes', { 'grebi:sourceIds': 'mondo:0005083', resolve: 'false' }])

    expect(await screen.findByDisplayValue('psoriasis')).toBeInTheDocument()
    expect(screen.getByText('Results')).toBeInTheDocument()
    await waitFor(() => expect(queryCalls()).toHaveLength(1))
    const [path, params] = queryCalls()[0]
    expect(path).toBe('api/v1/graphs/g/query/gwas_studies_by_disease')
    expect(params.get('disease_id')).toBe('mondo:0005083')
    expect(await screen.findByRole('link', { name: 'Study one' })).toBeInTheDocument()
  })

  it('takes text parameters from the query string, falling back to declared defaults', async () => {
    renderInterface(textTemplate, '?chrom=7')
    await waitFor(() => expect(queryCalls()).toHaveLength(1))
    const params = queryCalls()[0][1]
    expect(params.get('chrom')).toBe('7')
    expect(params.get('min_score')).toBe('0.5')
    expect(screen.getByDisplayValue('7')).toBeInTheDocument()
    expect(screen.getByDisplayValue('0.5')).toBeInTheDocument()
    expect(nodeCalls()).toHaveLength(0)
  })

  it('re-runs the query when an input is edited', async () => {
    renderInterface(textTemplate, '?chrom=7')
    await waitFor(() => expect(queryCalls()).toHaveLength(1))
    fireEvent.change(screen.getByDisplayValue('7'), { target: { value: '8' } })
    await waitFor(() => expect(queryCalls()).toHaveLength(2))
    expect(queryCalls()[1][1].get('chrom')).toBe('8')
    expect(queryCalls()[1][1].get('min_score')).toBe('0.5')
  })

  it('a template without parameters runs straight away', async () => {
    renderInterface(noParams)
    expect(screen.queryByText('Inputs')).toBeNull()
    expect(await screen.findByRole('link', { name: 'Study one' })).toBeInTheDocument()
    // exactly one query on mount: the submit waits for the query string to have been read
    await act(async () => {})
    expect(queryCalls()).toHaveLength(1)
    for (const [, params] of queryCalls()) {
      expect(params.toString()).toBe('page=0&size=10&resolve=false')
    }
  })

  it('renders the sidebar and gives only materialised templates the free-text filter', async () => {
    const { unmount } = renderInterface({ ...noParams, materialised: true }, '', <div>Side content</div>)
    expect(screen.getByText('Side content')).toBeInTheDocument()
    expect(await screen.findByPlaceholderText('Filter results…')).toBeInTheDocument()
    unmount()

    renderInterface({ ...noParams, materialised: false })
    await screen.findByRole('link', { name: 'Study one' })
    expect(screen.queryByPlaceholderText('Filter results…')).toBeNull()
  })
})
