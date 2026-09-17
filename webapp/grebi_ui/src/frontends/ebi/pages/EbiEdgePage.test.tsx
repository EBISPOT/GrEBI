import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import encodeNodeId from '../../../encodeNodeId'
import EbiEdgePage from './EbiEdgePage'

vi.mock('../../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, get: vi.fn(), getPaginated: vi.fn(), post: vi.fn() }
})
import { get, ApiError } from '../../../app/api'
const mockedGet = vi.mocked(get)

const edge = {
  'grebi:edgeId': 'edge-1',
  'grebi:type': 'biolink:has_phenotype',
  'grebi:datasources': ['HPOA'],
  'grebi:fromNodeId': 'n1',
  'grebi:toNodeId': 'n2',
  from: { 'grebi:nodeId': 'n1', 'grebi:name': ['Marfan syndrome'] },
  to: { 'grebi:nodeId': 'n2', 'grebi:name': ['Arachnodactyly'] },
  'hpoa:frequency': ['HP:0040282'],
  _refs: { 'HP:0040282': { 'grebi:nodeId': 'HP:0040282', 'grebi:name': ['Frequent'] } },
}

function renderPage(edgeId = 'edge-1') {
  return render(
    <MemoryRouter initialEntries={[`/graphs/g1/edges/${encodeNodeId(edgeId)}`]}>
      <Routes>
        <Route path="/graphs/:graph/edges/:edgeId" element={<EbiEdgePage />} />
      </Routes>
    </MemoryRouter>
  )
}

beforeEach(() => {
  mockedGet.mockReset()
  mockedGet.mockImplementation(async (path: string) => {
    if (path === `api/v1/graphs/g1/edges/${encodeNodeId('edge-1')}`) return edge
    if (path === 'api/v1/graphs') return ['g1']
    if (path === 'api/v1/stats') return {}
    if (path === 'api/v1/graphs/g1') return { subgraph_config: { name: 'Graph one' } }
    throw new ApiError(404, path, 'Edge not found')
  })
})

describe('EbiEdgePage', () => {
  it('shows the edge named in the URL: endpoints, type, datasources and properties', async () => {
    renderPage()
    expect(await screen.findByRole('link', { name: 'Marfan syndrome' })).toHaveAttribute('href', `/graphs/g1/nodes/${encodeNodeId('n1')}`)
    expect(screen.getByRole('link', { name: 'Arachnodactyly' })).toHaveAttribute('href', `/graphs/g1/nodes/${encodeNodeId('n2')}`)
    expect(screen.getAllByText('biolink:has_phenotype').length).toBeGreaterThan(0)
    expect(screen.getByTitle(/hpoa/i)).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Frequent' })).toBeInTheDocument()
    const crumbs = within(screen.getByLabelText('breadcrumb'))
    expect(crumbs.getByRole('link', { name: 'Edges' })).toHaveAttribute('href', '/graphs/g1/edges')
    expect(crumbs.getByText('biolink:has_phenotype')).toBeInTheDocument()
  })

  it('says when there is no such edge', async () => {
    renderPage('edge-9')
    const alert = await screen.findByRole('alert')
    expect(alert).toHaveTextContent('The edge not found')
    expect(alert).toHaveTextContent('Edge not found')
  })
})
