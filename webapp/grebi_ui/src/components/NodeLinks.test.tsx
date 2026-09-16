import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, act, waitFor } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import NodeLinks from './NodeLinks'
import GraphNode from '../model/GraphNode'
import encodeNodeId from '../encodeNodeId'
import { Page } from '../app/api'

const api = vi.hoisted(() => ({ getPaginated: vi.fn() }))

vi.mock('../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, getPaginated: api.getPaginated }
})

function LocationDisplay() {
  const loc = useLocation()
  return <div data-testid="location">{loc.pathname + loc.search}</div>
}

const gene = new GraphNode({
  'grebi:nodeId': 'n-brca2',
  'grebi:name': ['BRCA2'],
  'grebi:type': ['biolink:Gene'],
  'grebi:sourceIds': ['hgnc:1101', 'ensembl:ENSG00000139618'],
  _refs: {},
})
const disease = new GraphNode({
  'grebi:nodeId': 'n-psoriasis',
  'grebi:name': ['psoriasis'],
  'grebi:type': ['biolink:Disease'],
  'grebi:sourceIds': ['mondo:0005083'],
  _refs: {},
})

// an incoming chemical-gene edge as the API returns it
const interaction = {
  'grebi:edgeId': 'e1',
  'grebi:type': 'biolink:chemical_gene_interaction_association',
  'grebi:datasources': ['CTD'],
  'grebi:fromNodeId': 'n-aspirin',
  'grebi:toNodeId': 'n-brca2',
  from: { 'grebi:nodeId': 'n-aspirin', 'grebi:name': ['aspirin'], 'grebi:type': ['biolink:ChemicalEntity'] },
  to: { 'grebi:nodeId': 'n-brca2', 'grebi:name': ['BRCA2'] },
  'ctd:interaction_actions': 'increases^expression',
  'ctd:pubmed': 'pubmed:12345',
  _refs: { 'pubmed:12345': { 'grebi:nodeId': 'n-pub', 'grebi:name': ['Aspirin and BRCA2 expression'] } },
}

const edgesPath = `api/v1/graphs/g/nodes/${encodeNodeId('n-brca2')}/incoming_edges`
const typeFilter = { 'grebi:type': 'biolink:chemical_gene_interaction_association' }

function renderLinks(node: GraphNode, search = '') {
  return render(
    <MemoryRouter initialEntries={[`/graphs/g/nodes/x${search}`]}>
      <NodeLinks node={node} graph="g" />
      <LocationDisplay />
    </MemoryRouter>
  )
}

beforeEach(() => {
  api.getPaginated.mockReset()
  api.getPaginated.mockImplementation(async (_path: string, params: any) =>
    // the tab count asks for a single row; the tab body asks for the page
    params?.size === '1' ? new Page(0, 1, 7, 7, [interaction], new Map()) : new Page(0, 1, 1, 1, [interaction], new Map())
  )
})

describe('NodeLinks', () => {
  it('lists the source ids and has no extra tabs for a node that is not a gene', async () => {
    renderLinks(disease)
    expect(screen.getByRole('tab', { name: 'Source IDs' })).toBeInTheDocument()
    expect(screen.getByText('mondo:0005083')).toBeInTheDocument()
    await act(async () => {})
    expect(screen.getAllByRole('tab')).toHaveLength(1)
    expect(api.getPaginated).not.toHaveBeenCalled()
  })

  it('copies a source id to the clipboard', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    try {
      renderLinks(disease)
      fireEvent.click(screen.getByText('mondo:0005083').querySelector('button')!)
      expect(writeText).toHaveBeenCalledWith('mondo:0005083')
    } finally {
      delete (navigator as any).clipboard
    }
  })

  it('adds a chemical interactions tab for genes, counted with a one-row query', async () => {
    renderLinks(gene)
    expect(await screen.findByRole('tab', { name: 'Chemical Interactions' })).toBeInTheDocument()
    expect(api.getPaginated).toHaveBeenCalledTimes(1)
    expect(api.getPaginated).toHaveBeenCalledWith(edgesPath, { size: '1', ...typeFilter })
    expect(screen.getByText('hgnc:1101')).toBeInTheDocument()
    expect(screen.getByText('ensembl:ENSG00000139618')).toBeInTheDocument()
  })

  it('selecting the tab records it in the URL and loads the interactions', async () => {
    renderLinks(gene)
    fireEvent.click(await screen.findByRole('tab', { name: 'Chemical Interactions' }))
    expect(screen.getByTestId('location')).toHaveTextContent('?linksTab=chemical_gene_interactions')

    // the table re-keys its rows on every render, so always query afresh
    const chemical = () => screen.getByRole('link', { name: 'aspirin' })
    await waitFor(() => expect(chemical()).toHaveAttribute('href', `/graphs/g/nodes/${encodeNodeId('n-aspirin')}`))
    expect(api.getPaginated).toHaveBeenLastCalledWith(edgesPath, typeFilter)
    expect(screen.getByText('Chemical')).toBeInTheDocument()
    expect(screen.getByText('CTD')).toBeInTheDocument()
    // the remaining edge properties become columns, values resolved through _refs
    await waitFor(() => expect(screen.getByText('ctd:interaction_actions')).toBeInTheDocument())
    expect(screen.getByText(/increases\^expression/)).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Aspirin and BRCA2 expression' })).toBeInTheDocument()
    for (const hidden of ['grebi:fromNodeId', 'grebi:type', 'to']) {
      expect(screen.queryByText(hidden)).toBeNull()
    }
  })

  it('opens the tab named in the URL directly', async () => {
    renderLinks(gene, '?linksTab=chemical_gene_interactions')
    await waitFor(() => expect(screen.getByRole('link', { name: 'aspirin' })).toBeInTheDocument())
    expect(screen.queryByText('hgnc:1101')).toBeNull()
    expect(api.getPaginated).toHaveBeenLastCalledWith(edgesPath, typeFilter)
  })
})
