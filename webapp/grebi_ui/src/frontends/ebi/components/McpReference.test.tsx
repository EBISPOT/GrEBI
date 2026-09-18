import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import McpReference, { otherTemplates, toolsForGraph, resultColumns, propertyType, titleOf, McpCatalogue } from './McpReference'

const api = vi.hoisted(() => ({ get: vi.fn() }))
vi.mock('../../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, get: api.get, getPaginated: vi.fn(), post: vi.fn() }
})

const catalogue: McpCatalogue = {
  server: { name: 'grebi', version: '1.0.0', endpoint: '/api/v1/mcp', transport: 'streamable-http' },
  instructions: 'This is GrEBI.\n\nUse the query templates.',
  graphs: ['g1'],
  resources: [
    { uri: 'grebi://graphs', name: 'Graphs', mimeType: 'application/json' },
    { uri: 'grebi://stats', name: 'Knowledge Graph Statistics', description: 'Counts.', mimeType: 'application/json' },
  ],
  tools: [
    {
      name: 'search_nodes', description: 'Search for nodes by text.',
      inputSchema: { type: 'object', properties: { graph: { enum: ['g1'], description: 'Graph to search' }, q: { type: 'string', description: 'Text' }, pageSize: { type: 'integer', minimum: 1, maximum: 100 } }, required: ['graph'] },
      outputSchema: { type: 'object', properties: { rows: {}, totalNumRows: {} } },
    },
    {
      name: 'studies_by_trait', description: 'GWAS studies annotated with a trait: Studies whose mapped trait is the given term.',
      template: 'studies_by_trait', graphs: ['g1', 'g2'],
      inputSchema: { type: 'object', properties: { graph: { enum: ['g1', 'g2'] }, trait_id: { type: 'string', description: 'Trait' }, sortBy: { enum: ['study', 'p_value'] }, sortDir: { enum: ['asc', 'desc'] }, pageNum: { type: 'integer' }, pageSize: { type: 'integer' } }, required: ['graph', 'trait_id', 'sortBy', 'sortDir', 'pageNum', 'pageSize'] },
      outputSchema: { type: 'object', properties: { rows: { type: 'array', items: { type: 'object', properties: { study: { type: 'object' }, p_value: { type: ['number', 'null'] } } } }, totalNumRows: { type: 'integer' } } },
    },
    {
      name: 'elsewhere_only', description: 'Elsewhere: a question only another graph answers.', template: 'elsewhere_only', graphs: ['g3', 'g4'],
      inputSchema: { type: 'object', properties: { graph: { enum: ['g3', 'g4'] }, x: { type: 'string' } }, required: ['graph'] },
    },
    {
      name: 'node_count', description: 'Node count: how many nodes there are.', template: 'node_count',
      inputSchema: { type: 'object', properties: { graph: { enum: ['g1'] }, sortBy: { enum: ['n'] }, sortDir: { enum: ['asc', 'desc'] }, pageNum: { type: 'integer' }, pageSize: { type: 'integer' } }, required: ['graph'] },
      outputSchema: { type: 'object', properties: { rows: { type: 'array', items: { type: 'object', properties: { n: { type: 'integer' } } } } } },
    },
  ],
}

function renderReference(props: { catalogue?: McpCatalogue } = {}) {
  return render(<MemoryRouter><McpReference {...props} /></MemoryRouter>)
}

beforeEach(() => {
  api.get.mockReset()
})

describe('McpReference helpers', () => {
  it('gives a served graph the templates naming it and those for any graph', () => {
    expect(toolsForGraph(catalogue, 'g1').map((t) => t.name)).toEqual(['studies_by_trait', 'node_count'])
  })

  it('sets aside the templates none of whose graphs the instance serves', () => {
    expect(otherTemplates(catalogue).map((t) => t.name)).toEqual(['elsewhere_only'])
    expect(titleOf(catalogue.tools[2])).toBe('Elsewhere')
    expect(titleOf({ name: 'x', description: 'no colon' })).toBe('no colon')
  })

  it('reads result columns and types from the output schema', () => {
    expect(resultColumns(catalogue.tools[1])).toEqual([['study', 'object'], ['p_value', 'number | null']])
    expect(resultColumns(catalogue.tools[0])).toEqual([])
    expect(propertyType({ enum: ['a', 'b'] })).toBe('a | b')
    expect(propertyType({ type: 'array', items: { type: 'string' } })).toBe('array of string')
  })
})

describe('McpReference', () => {
  it('shows the server, its instructions, its resources and the fixed tools', () => {
    renderReference({ catalogue })
    expect(screen.getByText(/This instance serves/)).toHaveTextContent('g1')
    expect(screen.getByText('This is GrEBI.')).toBeInTheDocument()
    expect(screen.getByText('Use the query templates.')).toBeInTheDocument()
    const resources = screen.getByRole('heading', { name: 'Resources' }).nextElementSibling as HTMLElement
    expect(within(resources).getAllByRole('row')).toHaveLength(3)
    expect(within(resources).getByText('grebi://stats')).toBeInTheDocument()
    expect(within(resources).getByText(': Counts.')).toBeInTheDocument()

    const search = screen.getByText('search_nodes').closest('div.border') as HTMLElement
    expect(within(search).getByText('Search for nodes by text.')).toBeInTheDocument()
    const rows = within(search).getAllByRole('row').slice(1)
    expect(rows.map((r) => within(r).getAllByRole('cell')[0].textContent)).toEqual(['graph *', 'q', 'pageSize'])
    expect(within(rows[0]).getByText('g1')).toBeInTheDocument()
    expect(within(search).getByText(/Returns/)).toHaveTextContent('rows, totalNumRows')
  })

  it('lists the template tools under each served graph, and the others in one table', () => {
    renderReference({ catalogue })
    expect(screen.getAllByRole('heading', { level: 2 }).map((h) => h.textContent)).toEqual([
      'Instructions to agents', 'Resources', 'Tools for every graph', 'Tools for g1', 'Templates for other graphs',
    ])
    const g1 = screen.getByRole('heading', { name: 'Tools for g1' }).parentElement as HTMLElement
    expect(within(g1).getAllByText(/^(studies_by_trait|node_count|elsewhere_only)$/).map((e) => e.textContent)).toEqual(['studies_by_trait', 'node_count'])
    const others = screen.getByRole('heading', { name: 'Templates for other graphs' }).parentElement as HTMLElement
    const otherRows = within(others).getAllByRole('row').slice(1)
    expect(otherRows).toHaveLength(1)
    expect(within(otherRows[0]).getAllByRole('cell').map((c) => c.textContent)).toEqual(['elsewhere_only', 'Elsewhere', 'g3, g4'])

    const studies = within(g1).getByText('studies_by_trait').closest('div.border') as HTMLElement
    // only the template's own parameters are listed; the controls are described once in prose
    const rows = within(studies).getAllByRole('row').slice(1)
    expect(rows.map((r) => within(r).getAllByRole('cell')[0].textContent)).toEqual(['trait_id *'])
    expect(within(studies).getByText(/Sort by/)).toHaveTextContent('study, p_value')
    expect(within(studies).getByText(/Result columns/)).toHaveTextContent('study (object), p_value (number | null)')
    expect(within(studies).getByRole('link', { name: 'Run it in the browser' })).toHaveAttribute('href', '/graphs/g1/queries/studies_by_trait')

    const count = within(g1).getByText('node_count').closest('div.border') as HTMLElement
    expect(within(count).getByText('runs on any graph the instance serves')).toBeInTheDocument()
    expect(within(count).getByText('No arguments of its own.')).toBeInTheDocument()
  })

  it('fetches the catalogue from the API when none is given', async () => {
    api.get.mockResolvedValue(catalogue)
    renderReference()
    expect(screen.getByText('Loading the MCP catalogue…')).toBeInTheDocument()
    expect(await screen.findByText('search_nodes')).toBeInTheDocument()
    expect(api.get).toHaveBeenCalledWith('api/v1/mcp/catalogue')
  })

  it('says when the catalogue could not be loaded', async () => {
    const { ApiError } = await import('../../../app/api')
    api.get.mockRejectedValue(new ApiError(503, 'u', 'gone'))
    renderReference()
    expect(await screen.findByRole('alert')).toHaveTextContent('The MCP catalogue could not be loaded')
  })
})
