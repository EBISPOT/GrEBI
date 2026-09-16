import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import PropList from './PropList'
import PropVal from '../model/PropVal'
import Refs from '../model/Refs'

const refs = new Refs({
  'ro:0002200': { 'grebi:nodeId': 'ro:0002200', 'grebi:name': ['has phenotype'] },
  'hp:0000001': { 'grebi:nodeId': 'hp:0000001', 'grebi:name': ['All'] },
})

function renderList(props: Record<string, any[]>) {
  const asPropVals = Object.fromEntries(Object.entries(props).map(([k, v]) => [k, PropVal.arrFrom(v)]))
  return render(
    <MemoryRouter>
      <PropList graph="g" refs={refs} props={asPropVals} />
    </MemoryRouter>
  )
}

describe('PropList', () => {
  it('renders a friendly label and the values for each prop', () => {
    const { container } = renderList({ 'grebi:name': ['psoriasis', 'Psoriasis'], 'grebi:description': ['A skin disease'] })
    expect(screen.getByText('Name')).toBeInTheDocument()
    expect(screen.getByText('Description')).toBeInTheDocument()
    // values are separate spans, so check the flattened text
    expect(container.textContent).toContain('psoriasis; Psoriasis')
    expect(container.textContent).toContain('A skin disease')
  })

  it('labels props by their ref and links values that are refs', () => {
    renderList({ 'ro:0002200': ['hp:0000001'] })
    expect(screen.getByText('has phenotype')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'All' })).toHaveAttribute('href', '/graphs/g/nodes/aHA6MDAwMDAwMQ')
  })

  it('shows unknown props by their raw key', () => {
    renderList({ 'gwas:pvalue': ['1e-8'] })
    expect(screen.getByText('gwas:pvalue')).toBeInTheDocument()
    expect(screen.getByText(/1e-8/)).toBeInTheDocument()
  })

  it('hides internal keys', () => {
    const { container } = renderList({
      _refs: ['secret-refs'], 'grebi:edgeId': ['secret-edge'], 'grebi:fromNodeId': ['secret-from'],
      'grebi:toNodeId': ['secret-to'], 'grebi:nodeId': ['secret-node'], from: ['secret-f'], to: ['secret-t'],
      'grebi:name': ['visible'],
    })
    expect(container.textContent).not.toContain('secret')
    expect(container.textContent).toContain('visible')
  })
})
