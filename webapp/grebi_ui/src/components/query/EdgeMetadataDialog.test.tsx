import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import EdgeMetadataDialog from './EdgeMetadataDialog'
import encodeNodeId from '../../encodeNodeId'

const api = vi.hoisted(() => ({ get: vi.fn() }))

vi.mock('../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, get: api.get }
})

// A resolved edge as the API returns it: endpoints inlined, plus _refs for
// any property key or value that is itself a node.
const edge = {
  'grebi:edgeId': 'edge-1',
  'grebi:type': 'biolink:chemical_gene_interaction_association',
  'grebi:datasources': ['CTD'],
  'grebi:fromNodeId': 'n1',
  'grebi:toNodeId': 'n2',
  from: { 'grebi:nodeId': 'n1', 'grebi:name': ['aspirin'] },
  to: { 'grebi:nodeId': 'n2', 'grebi:name': ['PTGS2'] },
  'biolink:primary_knowledge_source': 'infores:ctd',
  'ctd:pubmed': ['12345'],
  _refs: {
    'infores:ctd': { 'grebi:nodeId': 'ctd-source', 'grebi:name': ['CTD source'] },
    'ctd:pubmed': { 'grebi:nodeId': 'ctd-pubmed', 'grebi:name': ['PubMed reference'] },
  },
}

type Props = Parameters<typeof EdgeMetadataDialog>[0]

function renderDialog(props: Partial<Props> = {}) {
  const onClose = vi.fn()
  const utils = render(
    <MemoryRouter>
      <EdgeMetadataDialog open graph="g" edgeId="edge-1" onClose={onClose} {...props} />
    </MemoryRouter>
  )
  return { onClose, ...utils }
}

beforeEach(() => {
  api.get.mockReset()
  api.get.mockResolvedValue(edge)
})

describe('EdgeMetadataDialog', () => {
  it('renders nothing and fetches nothing while closed', () => {
    const { container } = renderDialog({ open: false })
    expect(container).toBeEmptyDOMElement()
    expect(screen.queryByText('Edge Properties')).toBeNull()
    expect(api.get).not.toHaveBeenCalled()
  })

  it('loads the edge by encoded id and shows its endpoints, type and properties', async () => {
    renderDialog()
    expect(api.get).toHaveBeenCalledWith(`api/v1/graphs/g/edges/${encodeNodeId('edge-1')}`)

    const from = await screen.findByRole('link', { name: 'aspirin' })
    expect(from).toHaveAttribute('href', `/graphs/g/nodes/${encodeNodeId('n1')}`)
    expect(screen.getByRole('link', { name: 'PTGS2' })).toHaveAttribute('href', `/graphs/g/nodes/${encodeNodeId('n2')}`)

    // the type is shown in the header and again as a property, labelled "Type"
    expect(screen.getAllByText('biolink:chemical_gene_interaction_association')).toHaveLength(2)
    expect(screen.getByText('Type')).toBeInTheDocument()

    // keys and values that are in _refs are shown by name, values as node links
    expect(screen.getByText('PubMed reference')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'CTD source' })).toHaveAttribute(
      'href',
      `/graphs/g/nodes/${encodeNodeId('infores:ctd')}`
    )
    expect(screen.getByText(/12345/)).toBeInTheDocument()

    // structural keys are hidden
    for (const hidden of ['grebi:fromNodeId', 'grebi:toNodeId', 'grebi:edgeId', '_refs']) {
      expect(screen.queryByText(hidden)).toBeNull()
    }
  })

  it('links to the edge page and closes on the way', async () => {
    const { onClose } = renderDialog()
    await screen.findByRole('link', { name: 'aspirin' })
    const link = screen.getByRole('link', { name: /Open edge page/ })
    expect(link).toHaveAttribute('href', `/graphs/g/edges/${encodeNodeId('edge-1')}`)
    fireEvent.click(link)
    expect(onClose).toHaveBeenCalledTimes(1)
  })

  it('the close button reports back', async () => {
    const { onClose } = renderDialog()
    await screen.findByRole('link', { name: 'aspirin' })
    fireEvent.click(screen.getByTestId('CloseIcon').closest('button')!)
    expect(onClose).toHaveBeenCalledTimes(1)
  })

  it('shows the error when the edge cannot be loaded', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {})
    try {
      api.get.mockRejectedValue(new Error('Failure loading edge with status 404'))
      renderDialog()
      expect(await screen.findByText('Failure loading edge with status 404')).toBeInTheDocument()
      expect(screen.queryByText('Loading edge details...')).toBeNull()
      expect(screen.queryByText('Type')).toBeNull()
    } finally {
      error.mockRestore()
    }
  })

  it('opens empty without an edge id and does not query', () => {
    renderDialog({ edgeId: null })
    expect(screen.getByText('Edge Properties')).toBeInTheDocument()
    expect(api.get).not.toHaveBeenCalled()
    expect(screen.queryByText('Type')).toBeNull()
  })

  it('refetches when a different edge is requested', async () => {
    const { rerender } = renderDialog()
    await screen.findByRole('link', { name: 'aspirin' })

    api.get.mockResolvedValue({ ...edge, from: { 'grebi:nodeId': 'n3', 'grebi:name': ['ibuprofen'] } })
    rerender(
      <MemoryRouter>
        <EdgeMetadataDialog open graph="g" edgeId="edge-2" onClose={() => {}} />
      </MemoryRouter>
    )
    expect(await screen.findByRole('link', { name: 'ibuprofen' })).toBeInTheDocument()
    expect(screen.queryByRole('link', { name: 'aspirin' })).toBeNull()
    expect(api.get).toHaveBeenLastCalledWith(`api/v1/graphs/g/edges/${encodeNodeId('edge-2')}`)
  })
})
