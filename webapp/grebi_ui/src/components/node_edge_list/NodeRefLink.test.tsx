import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import NodeRefLink from './NodeRefLink'
import GraphNodeRef from '../../model/GraphNodeRef'
import encodeNodeId from '../../encodeNodeId'

function renderLink(props: any, showTypeChip?: boolean) {
  return render(
    <MemoryRouter>
      <NodeRefLink graph="g" nodeRef={new GraphNodeRef(props)} showTypeChip={showTypeChip} />
    </MemoryRouter>
  )
}

describe('NodeRefLink', () => {
  it('links to the node page by encoded node id, labelled with the display name', () => {
    renderLink({ 'grebi:nodeId': 'ebi_monarch_xspecies:mondo:0005083', 'grebi:name': ['psoriasis', 'Psoriasis'] })
    const link = screen.getByRole('link', { name: 'psoriasis' })
    expect(link).toHaveAttribute('href', `/graphs/g/nodes/${encodeNodeId('ebi_monarch_xspecies:mondo:0005083')}`)
    // base64 padding is stripped from the url
    expect(link.getAttribute('href')).not.toMatch(/=/)
  })

  it('shows a type chip only when asked', () => {
    const node = { 'grebi:nodeId': 'n', 'grebi:name': ['BRCA2'], 'grebi:type': ['biolink:Gene'] }
    const { unmount } = renderLink(node, true)
    expect(screen.getByRole('link')).toHaveTextContent('BRCA2Gene')
    unmount()

    renderLink(node)
    expect(screen.getByRole('link')).toHaveTextContent(/^BRCA2$/)
  })

  it('omits the chip for an unknown type even when asked', () => {
    renderLink({ 'grebi:nodeId': 'n', 'grebi:name': ['evidence'], 'grebi:type': ['otar:Evidence'] }, true)
    expect(screen.getByRole('link')).toHaveTextContent(/^evidence$/)
  })

  it('falls back to the id when the node has no name', () => {
    renderLink({ 'grebi:nodeId': 'n', 'grebi:curie': 'hgnc:1101' })
    expect(screen.getByRole('link', { name: 'hgnc:1101' })).toBeInTheDocument()
  })
})
