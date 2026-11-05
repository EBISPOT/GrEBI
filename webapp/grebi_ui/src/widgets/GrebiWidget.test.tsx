import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, within, waitFor } from '@testing-library/react'
import GrebiWidget from './GrebiWidget'
import { configureInstance } from '../app/instance'

// sigma (the graph view) needs WebGL, which jsdom has not; the node page is not under test here
vi.mock('../components/node_graph_view/GraphView', () => ({ default: () => <div data-testid="graph-view" /> }))

vi.mock('../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, get: vi.fn(), getPaginated: vi.fn(), post: vi.fn() }
})
import { get, getPaginated, Page } from '../app/api'
const mockedGet = vi.mocked(get)
const mockedGetPaginated = vi.mocked(getPaginated)

const template = {
  id: 'q1',
  title: 'Diseases associated with a gene',
  question: 'Which [diseases]{disease} are associated with gene {gene_symbol}?',
  description: '',
  graphs: ['g1'],
  topics: [],
  cypher_match_fragment: 'MATCH (g:Gene)',
  cypher_return_fragment: 'RETURN d AS disease',
  cypher_count_fragment: '',
  params: [{ param_id: 'gene_symbol', param_name: 'Gene symbol', param_type: 'string' }],
  result_columns: [{ column_id: 'disease', column_type: 'GraphNodeId' }],
  examples: [],
}
const rows = [{ disease: { 'grebi:nodeId': 'mondo:0005083', 'grebi:name': ['psoriasis'] } }]

let host: HTMLElement
let shadow: ShadowRoot
let container: HTMLElement

beforeEach(() => {
  configureInstance({ siteUrl: 'https://kg.example/spot/kg/' })
  host = document.createElement('div')
  document.body.appendChild(host)
  shadow = host.attachShadow({ mode: 'open' })
  container = document.createElement('div')
  shadow.appendChild(container)
  mockedGet.mockReset()
  mockedGetPaginated.mockReset()
  mockedGet.mockImplementation(async (path: string) => {
    if (path === 'api/v1/graphs/g1/query_templates/q1') return template
    if (path === 'api/v1/topics') return []
    if (path === 'api/v1/graphs') return ['g1']
    if (path === 'api/v1/stats') return {}
    throw new Error('unexpected GET ' + path)
  })
  mockedGetPaginated.mockResolvedValue(new Page<any>(0, 1, 1, 1, rows, new Map()))
})

afterEach(() => {
  host.remove()
  configureInstance({ siteUrl: undefined })
})

describe('GrebiWidget', () => {
  it('shows the page at the path in the widget layout, inside the shadow root', async () => {
    render(<GrebiWidget path="/graphs/g1/queries/q1?gene_symbol=BRCA1" shadowRoot={shadow} container={container} />, { container })
    const inside = within(container)
    expect(await inside.findByRole('heading', { name: 'Diseases associated with a gene' })).toBeInTheDocument()
    expect(await inside.findByRole('link', { name: 'psoriasis' })).toBeInTheDocument()
    expect(inside.getByRole('link', { name: 'Open in GrEBI ↗' })).toHaveAttribute('href', 'https://kg.example/spot/kg/graphs/g1/queries/q1?gene_symbol=BRCA1')
    // a widget: no site navigation, no breadcrumbs, results without the inputs
    expect(inside.queryByRole('menubar')).toBeNull()
    expect(inside.queryByLabelText('breadcrumb')).toBeNull()
    expect(inside.queryByRole('heading', { name: 'Inputs' })).toBeNull()
    // the host document is untouched
    expect(document.body.querySelector('[aria-label="breadcrumb"]')).toBeNull()
    expect(document.title).toBe('')
  })

  it('keeps the styles MUI generates inside the shadow root', async () => {
    render(<GrebiWidget path="/graphs/g1/queries/q1?gene_symbol=BRCA1" shadowRoot={shadow} container={container} />, { container })
    await within(container).findByRole('heading', { name: 'Diseases associated with a gene' })
    await waitFor(() => expect(shadow.querySelector('style[data-emotion^="grebi"]')).not.toBeNull())
    expect(document.head.querySelector('style[data-emotion^="grebi"]')).toBeNull()
  })

  it('moves to another page when the path changes', async () => {
    const { rerender } = render(<GrebiWidget path="/graphs/g1/queries/q1?gene_symbol=BRCA1" shadowRoot={shadow} container={container} />, { container })
    await within(container).findByRole('heading', { name: 'Diseases associated with a gene' })
    rerender(<GrebiWidget path="/no/such/page" shadowRoot={shadow} container={container} />)
    await waitFor(() => expect(within(container).queryByRole('heading', { name: 'Diseases associated with a gene' })).toBeNull())
    expect(within(container).getByRole('link', { name: 'Open in GrEBI ↗' })).toHaveAttribute('href', 'https://kg.example/spot/kg/no/such/page')
  })
})
