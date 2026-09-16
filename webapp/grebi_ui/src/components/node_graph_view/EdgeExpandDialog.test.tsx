import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor, within, cleanup } from '@testing-library/react'
import EdgeExpandDialog from './EdgeExpandDialog'

vi.mock('../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, getPaginated: vi.fn() }
})

import { getPaginated, Page } from '../../app/api'
const mockedGetPaginated = vi.mocked(getPaginated)

const edge = (id: string, from: string, to: string, dss = ['ds1']) => ({
  'grebi:edgeId': id,
  'grebi:type': 'has_part',
  'grebi:datasources': dss,
  from: { 'grebi:nodeId': from, 'grebi:name': [from.toUpperCase()] },
  to: { 'grebi:nodeId': to, 'grebi:name': [to.toUpperCase()] },
})

const page = (elements: any[], total = elements.length, facets: any = {}) =>
  new Page<any>(0, elements.length, 1, total, elements, facets)

// the request path is a relative URL with a query string; parse it for assertions
function lastRequest() {
  const path = mockedGetPaginated.mock.calls.at(-1)![0]
  const url = new URL(path, 'http://x/')
  return { pathname: url.pathname, params: url.searchParams }
}

function renderDialog(overrides: Partial<React.ComponentProps<typeof EdgeExpandDialog>> = {}) {
  const props = {
    open: true,
    onClose: vi.fn(),
    onSelectNode: vi.fn(),
    graph: 'g',
    nodeId: 'root',
    encodedNodeId: 'ENC',
    direction: 'outgoing' as const,
    edgeType: 'has_part',
    parentLabel: 'Root label',
    ...overrides,
  }
  render(<EdgeExpandDialog {...props} />)
  return props
}

beforeEach(() => {
  mockedGetPaginated.mockReset()
  mockedGetPaginated.mockResolvedValue(page([edge('e1', 'a', 'b'), edge('e2', 'a', 'c', ['ds2'])], 2, { 'grebi:datasources': { ds1: 1, ds2: 1 } }))
})

describe('EdgeExpandDialog', () => {
  it('requests outgoing edge refs with paging, sorting and the edge type, and lists the target nodes', async () => {
    renderDialog()
    expect(await screen.findByText('B')).toBeInTheDocument()
    expect(screen.getByText('C')).toBeInTheDocument()
    expect(screen.getByText('To Node')).toBeInTheDocument()

    const { pathname, params } = lastRequest()
    expect(pathname).toBe('/api/v1/graphs/g/nodes/ENC/outgoing_edge_refs')
    expect(Object.fromEntries(params)).toEqual({ page: '0', size: '10', sortBy: 'grebi:type', sortDir: 'asc', 'grebi:type': 'has_part' })
    // title reads subject -> edge type -> (nodes)
    expect(screen.getByText('Root label')).toBeInTheDocument()
  })

  it('uses incoming edge refs and shows the source nodes for incoming edges', async () => {
    renderDialog({ direction: 'incoming' })
    expect(await screen.findAllByText('A')).toHaveLength(2)
    expect(screen.getByText('From Node')).toBeInTheDocument()
    expect(lastRequest().pathname).toBe('/api/v1/graphs/g/nodes/ENC/incoming_edge_refs')
  })

  it('hands back the node at the other end of the clicked row and closes', async () => {
    const props = renderDialog()
    fireEvent.click(await screen.findByText('C'))
    expect(props.onSelectNode).toHaveBeenCalledTimes(1)
    expect(vi.mocked(props.onSelectNode).mock.calls[0][0].getNodeId()).toBe('c')
    expect(props.onClose).toHaveBeenCalled()

    cleanup()
    const incoming = renderDialog({ direction: 'incoming' })
    fireEvent.click((await screen.findAllByText('A'))[0])
    expect(vi.mocked(incoming.onSelectNode).mock.calls[0][0].getNodeId()).toBe('a')
  })

  it('re-requests with a q parameter when the table is filtered', async () => {
    renderDialog()
    await screen.findByText('B')
    fireEvent.change(screen.getByPlaceholderText('Search...'), { target: { value: 'bet' } })
    await waitFor(() => expect(lastRequest().params.get('q')).toBe('bet'))
  })

  it('offers the datasource facets and excludes unticked datasources from the request', async () => {
    renderDialog()
    await screen.findByText('B')
    // the rows carry datasource tags with the same title; the selector's tag sits next to a checkbox
    const selectorRow = screen.getAllByTitle('ds2').map((el) => el.parentElement!).find((p) => within(p).queryByRole('checkbox'))!
    const ds2 = within(selectorRow).getByRole('checkbox')
    expect(ds2).toBeChecked()
    fireEvent.click(ds2)
    await waitFor(() => expect(lastRequest().params.getAll('-grebi:datasources')).toEqual(['ds2']))
    expect(lastRequest().params.get('grebi:type')).toBe('has_part')
  })

  it('does not fetch anything while closed', () => {
    renderDialog({ open: false })
    expect(mockedGetPaginated).not.toHaveBeenCalled()
  })
})
