import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import EbiDatasourcesPage from './EbiDatasourcesPage'

vi.mock('../../../app/api', () => ({ get: vi.fn(), getPaginated: vi.fn(), post: vi.fn() }))
import { get } from '../../../app/api'
const mockedGet = vi.mocked(get)

let graphsDeferred: ((v: string[]) => void) | null = null

beforeEach(() => {
  graphsDeferred = null
  mockedGet.mockReset()
  mockedGet.mockImplementation(async (path: string) => {
    if (path === 'api/v1/stats') return { g1: { num_nodes: 1000, num_edges: 2000 } }
    if (path === 'api/v1/graphs') {
      if (graphsDeferred !== null) return new Promise<string[]>((r) => { graphsDeferred = r })
      return ['g1', 'g2']
    }
    if (path === 'api/v1/graphs/g1') {
      return { subgraph_config: { name: 'Graph One', description: 'The first graph', datasource_configs: [{ id: 'a' }] } }
    }
    if (path === 'api/v1/graphs/g2') throw new Error('no metadata')
    throw new Error('unexpected GET ' + path)
  })
})

const renderPage = () => render(<MemoryRouter><EbiDatasourcesPage /></MemoryRouter>)

describe('EbiDatasourcesPage', () => {
  it('shows a spinner until the graph list arrives', () => {
    graphsDeferred = () => {}
    const { container } = renderPage()
    expect(container.querySelector('.spinner-default')).not.toBeNull()
    expect(document.title).toBe('Graphs - GrEBI')
    expect(screen.getByRole('link', { name: 'Graphs' })).toHaveAttribute('href', '/graphs')
  })

  it('renders a card per graph with name, datasource count, stats and description once metadata loads', async () => {
    renderPage()
    const one = await screen.findByRole('link', { name: /Graph One/ })
    expect(one).toHaveAttribute('href', '/graphs/g1')
    expect(one).toHaveTextContent('(g1)')
    expect(one).toHaveTextContent('1 datasource ·')
    expect(one).toHaveTextContent('1,000 nodes · 2,000 edges')
    expect(one).toHaveTextContent('The first graph')
  })

  it('falls back to the id, zero datasources and no stats for a graph without metadata', async () => {
    renderPage()
    const two = await screen.findByRole('link', { name: /\(g2\)/ })
    expect(two).toHaveAttribute('href', '/graphs/g2')
    expect(two).toHaveTextContent('g2(g2)0 datasources')
    expect(two).not.toHaveTextContent('nodes')
  })
})
