import { describe, it, expect, vi, beforeEach, beforeAll } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import EdgeExpandPanel from './EdgeExpandPanel'

vi.mock('../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, getPaginated: vi.fn() }
})

import { getPaginated, Page } from '../../app/api'
const mockedGetPaginated = vi.mocked(getPaginated)

// jsdom has no 2D canvas; record the drawing calls instead
const ctx: any = {
  scale: vi.fn(), clearRect: vi.fn(), beginPath: vi.fn(), moveTo: vi.fn(), lineTo: vi.fn(),
  stroke: vi.fn(), fill: vi.fn(), arc: vi.fn(), setLineDash: vi.fn(), fillText: vi.fn(),
  measureText: vi.fn(() => ({ width: 10 })),
}
beforeAll(() => {
  vi.spyOn(HTMLCanvasElement.prototype, 'getContext').mockImplementation(() => ctx)
})

const edge = (id: string, from: string, to: string, dss = ['ds1']) => ({
  'grebi:edgeId': id,
  'grebi:type': 'has_part',
  'grebi:datasources': dss,
  from: { 'grebi:nodeId': from, 'grebi:name': [from.toUpperCase()] },
  to: { 'grebi:nodeId': to, 'grebi:name': [to.toUpperCase()] },
})

const page = (elements: any[], total = elements.length) => new Page<any>(0, elements.length, 1, total, elements, new Map())

function lastRequest() {
  const url = new URL(mockedGetPaginated.mock.calls.at(-1)![0], 'http://x/')
  return { pathname: url.pathname, params: url.searchParams }
}

function renderPanel(overrides: Partial<React.ComponentProps<typeof EdgeExpandPanel>> = {}) {
  const props = {
    onSelectNode: vi.fn(),
    onCancel: vi.fn(),
    graph: 'g',
    nodeId: 'root',
    encodedNodeId: 'ENC',
    direction: 'outgoing' as const,
    edgeType: 'has_part',
    chain: [],
    rootLabel: 'Root',
    ...overrides,
  }
  render(<EdgeExpandPanel {...props} />)
  return props
}

beforeEach(() => {
  mockedGetPaginated.mockReset()
  for (const fn of Object.values(ctx)) (fn as any).mockClear?.()
})

describe('EdgeExpandPanel', () => {
  it('lists each target node once and excludes filtered datasources from the request', async () => {
    mockedGetPaginated.mockResolvedValue(page([edge('e1', 'a', 'b'), edge('e2', 'a', 'b', ['ds2']), edge('e3', 'a', 'c')]))
    renderPanel({ dsExclude: new Set(['ds2']) })

    expect(await screen.findByText('B')).toBeInTheDocument()
    expect(screen.getAllByText(/^[BC]$/)).toHaveLength(2)

    const { pathname, params } = lastRequest()
    expect(pathname).toBe('/api/v1/graphs/g/nodes/ENC/outgoing_edge_refs')
    expect(Object.fromEntries(params)).toEqual({ page: '0', size: '50', 'grebi:type': 'has_part', '-grebi:datasources': 'ds2' })
  })

  it('lists source nodes for incoming edges and hands the clicked node back', async () => {
    mockedGetPaginated.mockResolvedValue(page([edge('e1', 'a', 'b')]))
    const props = renderPanel({ direction: 'incoming' })
    fireEvent.click(await screen.findByText('A'))
    expect(props.onSelectNode.mock.calls[0][0].getNodeId()).toBe('a')
    expect(lastRequest().pathname).toBe('/api/v1/graphs/g/nodes/ENC/incoming_edge_refs')
  })

  it('says when there is nothing to expand', async () => {
    mockedGetPaginated.mockResolvedValue(page([]))
    renderPanel()
    expect(await screen.findByText('No nodes found')).toBeInTheDocument()
  })

  it('pages through large result sets', async () => {
    mockedGetPaginated.mockResolvedValue(page([edge('e1', 'a', 'b')], 120))
    renderPanel()
    // NOTE: current behaviour, looks like a bug: the range separator is written as
    // "–" inside JSX text, which is not an escape sequence there, so the
    // literal characters "–" are rendered instead of an en dash.
    expect(await screen.findByText('1\\u201350 of 120')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Prev' })).toBeDisabled()

    fireEvent.click(screen.getByRole('button', { name: 'Next' }))
    await waitFor(() => expect(lastRequest().params.get('page')).toBe('1'))
    expect(await screen.findByText('51\\u2013100 of 120')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Prev' })).toBeEnabled()
  })

  it('has no pagination when everything fits on one page', async () => {
    mockedGetPaginated.mockResolvedValue(page([edge('e1', 'a', 'b')], 50))
    renderPanel()
    await screen.findByText('B')
    expect(screen.queryByRole('button', { name: 'Next' })).toBeNull()
  })

  it('draws the chain preview: root, each segment, and a pending dashed node', async () => {
    mockedGetPaginated.mockResolvedValue(page([]))
    renderPanel({
      chain: [{ label: 'Child', edgeType: 'regulates', direction: 'outgoing' }],
      edgeType: 'a_very_long_edge_type_name',
    })
    await screen.findByText('No nodes found')

    const drawn = ctx.fillText.mock.calls.map((c: any[]) => c[0])
    expect(drawn).toContain('Root')
    expect(drawn).toContain('Child')
    expect(drawn).toContain('regulates')
    expect(drawn).toContain('?')
    // long edge labels are truncated with an ellipsis
    expect(drawn).toContain('a_very_long_edge…')
    // the pending edge and node are dashed
    expect(ctx.setLineDash).toHaveBeenCalledWith([4, 3])
    expect(ctx.setLineDash).toHaveBeenCalledWith([5, 4])
  })
})
