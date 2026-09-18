import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import OpenApiReference, { operationParameters, schemaLabel, tryIt } from './OpenApiReference'

const api = vi.hoisted(() => ({ get: vi.fn() }))
vi.mock('../../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, get: api.get, getPaginated: vi.fn(), post: vi.fn() }
})

const spec = {
  openapi: '3.1.0',
  info: { title: 'GrEBI API', description: 'Graphs.\n\n**Ids** are `Base64url`.' },
  tags: [{ name: 'Nodes', description: 'A node and its edges.' }, { name: 'Search' }, { name: 'Unused' }],
  paths: {
    '/api/v1/graphs/{graph}/nodes/{nodeId}': {
      parameters: [{ $ref: '#/components/parameters/graph' }, { $ref: '#/components/parameters/nodeId' }],
      get: {
        tags: ['Nodes'], summary: 'One node in full', description: 'Every property.',
        parameters: [{ name: 'lang', in: 'query', description: 'A language such as `fr`.', schema: { type: 'string' } }],
        responses: { '200': { description: 'The node.' }, '404': { $ref: '#/components/responses/NotFound' } },
      },
    },
    '/api/v1/graphs/{graph}/edges/{edgeId}': {
      parameters: [{ $ref: '#/components/parameters/graph' }, { name: 'edgeId', in: 'path', required: true, schema: { type: 'string' } }],
      get: { tags: ['Nodes'], summary: 'One edge', responses: { '200': { description: 'The edge.' } } },
    },
    '/api/v1/graphs/{graph}/lookup': {
      parameters: [{ $ref: '#/components/parameters/graph' }],
      post: {
        tags: ['Search'], summary: 'Look up identifiers', deprecated: false,
        parameters: [{ name: 'matchNames', in: 'query', schema: { type: 'boolean', default: false } }],
        requestBody: { required: true, description: 'The identifiers.', content: { 'application/json': { schema: { type: 'object' }, example: { ids: ['hgnc:1100'] } } } },
        responses: { '200': { description: 'The answer.' }, '413': { description: 'Too big.', content: { 'application/json': {} } } },
      },
    },
    '/api/v1/graphs/{graph}/search.csv': {
      parameters: [{ $ref: '#/components/parameters/graph' }],
      get: {
        tags: ['Search'], summary: 'Hits as CSV',
        parameters: [{ name: 'q', in: 'query', schema: { type: 'string' }, example: 'BRCA1' }, { name: 'grebi:type', in: 'query', schema: { type: 'array', items: { type: 'string' } } }],
        responses: { '200': { description: 'Rows.', content: { 'text/csv': { schema: { type: 'string' } } } } },
      },
    },
  },
  components: {
    parameters: {
      graph: { name: 'graph', in: 'path', required: true, description: 'The graph.', schema: { type: 'string' }, example: 'dismech' },
      nodeId: { name: 'nodeId', in: 'path', required: true, schema: { type: 'string' }, example: 'aGduYzoxMTAw' },
    },
    responses: { NotFound: { description: 'No such thing.' } },
  },
}

beforeEach(() => {
  api.get.mockReset()
})

describe('OpenApiReference helpers', () => {
  it('resolves parameter references, path-level first', () => {
    const item = spec.paths['/api/v1/graphs/{graph}/nodes/{nodeId}']
    expect(operationParameters(spec, item, item.get).map((p) => p.name)).toEqual(['graph', 'nodeId', 'lang'])
  })

  it('labels schemas briefly', () => {
    expect(schemaLabel(spec, { type: 'array', items: { type: 'string' } })).toBe('array of string')
    expect(schemaLabel(spec, { $ref: '#/components/schemas/Node' })).toBe('Node')
    expect(schemaLabel(spec, { type: 'string', enum: ['asc', 'desc'] })).toBe('asc | desc')
    expect(schemaLabel(spec, undefined)).toBe('')
  })

  it('builds a URL to try from the examples, or nothing when a path parameter has none', () => {
    const item = spec.paths['/api/v1/graphs/{graph}/nodes/{nodeId}']
    expect(tryIt('/api/v1/graphs/{graph}/nodes/{nodeId}', operationParameters(spec, item, item.get)))
      .toEqual({ url: '/api/v1/graphs/dismech/nodes/aGduYzoxMTAw', query: {} })
    const edge = spec.paths['/api/v1/graphs/{graph}/edges/{edgeId}']
    expect(tryIt('/api/v1/graphs/{graph}/edges/{edgeId}', operationParameters(spec, edge, edge.get))).toBeNull()
    const csv = spec.paths['/api/v1/graphs/{graph}/search.csv']
    expect(tryIt('/api/v1/graphs/{graph}/search.csv', operationParameters(spec, csv, csv.get))).toEqual({ url: '/api/v1/graphs/dismech/search.csv', query: { q: 'BRCA1' } })
  })
})

describe('OpenApiReference', () => {
  it('renders every operation under its tag, with parameters and responses', () => {
    render(<OpenApiReference spec={spec} />)
    // the introduction, with code spans
    expect(screen.getByText('Base64url').tagName).toBe('CODE')
    expect(screen.getByText('Ids').tagName).toBe('STRONG')
    // tags in the declared order, unused ones left out
    expect(screen.getAllByRole('heading', { level: 2 }).map((h) => h.textContent)).toEqual(['Nodes', 'Search'])
    expect(screen.getByText('A node and its edges.')).toBeInTheDocument()

    const node = screen.getByText('One node in full').closest('div.border') as HTMLElement
    expect(within(node).getByText('get')).toBeInTheDocument()
    expect(within(node).getByText('/api/v1/graphs/{graph}/nodes/{nodeId}')).toBeInTheDocument()
    const rows = within(node).getAllByRole('row').slice(1)
    expect(rows.map((r) => within(r).getAllByRole('cell')[0].textContent)).toEqual(['graph *', 'nodeId *', 'lang'])
    expect(within(rows[0]).getByText('The graph.')).toBeInTheDocument()
    expect(within(rows[0]).getByText('dismech').tagName).toBe('CODE')
    // a referenced response is resolved
    expect(within(node).getByText('No such thing.')).toBeInTheDocument()
  })

  it('offers to try a GET whose path parameters have examples, and a curl line for a POST', () => {
    render(<OpenApiReference spec={spec} />)
    const node = screen.getByText('One node in full').closest('div.border') as HTMLElement
    // the api-example block: a Try it button and the example URL
    expect(within(node).getByRole('button', { name: /Try it/ })).toBeInTheDocument()
    expect(within(node).getAllByText('/api/v1/graphs/dismech/nodes/aGduYzoxMTAw').length).toBeGreaterThan(0)

    const edge = screen.getByText('One edge').closest('div.border') as HTMLElement
    expect(within(edge).queryByRole('button', { name: /Try it/ })).toBeNull()

    const lookup = screen.getByText('Look up identifiers').closest('div.border') as HTMLElement
    expect(within(lookup).getByText('post')).toBeInTheDocument()
    expect(within(lookup).getByText(/Request body/)).toBeInTheDocument()
    expect(within(lookup).getByText(/Default/)).toBeInTheDocument()
    expect(within(lookup).getByTestId('curl').textContent).toContain(`-d '{"ids":["hgnc:1100"]}'`)
    expect(within(lookup).queryByRole('button', { name: /Try it/ })).toBeNull()
    // a non-JSON response says its content type
    const csv = screen.getByText('Hits as CSV').closest('div.border') as HTMLElement
    expect(within(csv).getByText('(text/csv)')).toBeInTheDocument()
  })

  it('fetches the description from the API when none is given', async () => {
    api.get.mockResolvedValue(spec)
    render(<OpenApiReference />)
    expect(screen.getByText('Loading the API description…')).toBeInTheDocument()
    expect(await screen.findByText('One node in full')).toBeInTheDocument()
    expect(api.get).toHaveBeenCalledWith('api/v1/openapi.json')
  })

  it('says when the description could not be loaded', async () => {
    const { ApiError } = await import('../../../app/api')
    api.get.mockRejectedValue(new ApiError(502, 'u', '502 Bad Gateway'))
    render(<OpenApiReference />)
    expect(await screen.findByRole('alert')).toHaveTextContent('The API description could not be loaded')
  })
})
