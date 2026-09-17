import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import NodeLinks from './NodeLinks'
import GraphNode from '../model/GraphNode'
import encodeNodeId from '../encodeNodeId'
import { Page } from '../app/api'
import { LinksTab } from './getNodeLinksTabs'

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
const chemicalTab: LinksTab = { tabId: 'chemical_gene_interactions', tabName: 'Chemical Interactions', count: 7 }

function renderLinks(tabs: LinksTab[], search = '') {
  return render(
    <MemoryRouter initialEntries={[`/graphs/g/nodes/x${search}`]}>
      <NodeLinks node={gene} graph="g" tabs={tabs} />
      <LocationDisplay />
    </MemoryRouter>
  )
}

beforeEach(() => {
  api.getPaginated.mockReset()
  api.getPaginated.mockResolvedValue(new Page(0, 1, 1, 1, [interaction], new Map()))
})

describe('NodeLinks', () => {
  it('opens the first link tab, with its count, and loads the interactions', async () => {
    renderLinks([chemicalTab])
    expect(screen.getByRole('tab', { name: 'Chemical Interactions (7)' })).toHaveAttribute('aria-selected', 'true')

    // the table re-keys its rows on every render, so always query afresh
    const chemical = () => screen.getByRole('link', { name: 'aspirin' })
    await waitFor(() => expect(chemical()).toHaveAttribute('href', `/graphs/g/nodes/${encodeNodeId('n-aspirin')}`))
    expect(api.getPaginated).toHaveBeenCalledTimes(1)
    expect(api.getPaginated).toHaveBeenCalledWith(edgesPath, typeFilter)
    expect(screen.getByText('Chemical')).toBeInTheDocument()
    expect(screen.getByText('CTD')).toBeInTheDocument()
    // the remaining edge properties become columns, values resolved through _refs
    await waitFor(() => expect(screen.getByText('ctd:interaction_actions')).toBeInTheDocument())
    expect(screen.getByText(/increases\^expression/)).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Aspirin and BRCA2 expression' })).toBeInTheDocument()
    for (const hidden of ['grebi:fromNodeId', 'grebi:type', 'to']) {
      expect(screen.queryByText(hidden)).toBeNull()
    }
    // the source ids live in the page header now, not here
    expect(screen.queryByText('hgnc:1101')).toBeNull()
  })

  it('records the chosen tab in the URL beside the page parameters', async () => {
    const other: LinksTab = { tabId: 'other_links', tabName: 'Other', count: 2 }
    renderLinks([other, chemicalTab], '?tab=links&lang=fr')
    expect(screen.getByRole('tab', { name: 'Other (2)' })).toHaveAttribute('aria-selected', 'true')
    expect(api.getPaginated).not.toHaveBeenCalled()

    fireEvent.click(screen.getByRole('tab', { name: 'Chemical Interactions (7)' }))
    expect(screen.getByTestId('location')).toHaveTextContent('?tab=links&lang=fr&linksTab=chemical_gene_interactions')
    await waitFor(() => expect(screen.getByRole('link', { name: 'aspirin' })).toBeInTheDocument())
  })

  it('opens the tab named in the URL, falling back to the first for an unknown one', async () => {
    const other: LinksTab = { tabId: 'other_links', tabName: 'Other', count: 2 }
    renderLinks([other, chemicalTab], '?linksTab=chemical_gene_interactions')
    expect(screen.getByRole('tab', { name: 'Chemical Interactions (7)' })).toHaveAttribute('aria-selected', 'true')
    await waitFor(() => expect(screen.getByRole('link', { name: 'aspirin' })).toBeInTheDocument())

    const { unmount } = renderLinks([other, chemicalTab], '?linksTab=nope')
    expect(screen.getAllByRole('tab', { name: 'Other (2)' }).at(-1)).toHaveAttribute('aria-selected', 'true')
    unmount()
  })

  it('says so when a node has no links', () => {
    renderLinks([])
    expect(screen.getByText('This node has no links of its own.')).toBeInTheDocument()
    expect(screen.queryByRole('tab')).toBeNull()
  })
})
