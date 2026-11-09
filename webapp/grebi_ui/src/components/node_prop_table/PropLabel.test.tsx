import { describe, it, expect } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import PropLabel from './PropLabel'
import Refs from '../../model/Refs'

const refs = new Refs({
  'ro:0002200': {
    'grebi:nodeId': 'ro:0002200',
    'grebi:name': ['RO_0002200', 'has phenotype'],
    'grebi:sourceIds': ['ro:0002200', 'RO_0002200'],
  },
  'grebi:name': { 'grebi:nodeId': 'grebi:name', 'grebi:name': ['a ref that should be ignored'] },
})

describe('PropLabel', () => {
  it('uses friendly labels for the core grebi props, even when a ref exists', () => {
    const expected = { 'grebi:name': 'Name', 'grebi:synonym': 'Synonym', 'grebi:description': 'Description', 'grebi:type': 'Type' }
    for (const [prop, label] of Object.entries(expected)) {
      const { unmount } = render(<PropLabel prop={prop} refs={refs} />)
      expect(screen.getByText(label).tagName).toBe('B')
      unmount()
    }
    expect(screen.queryByText('a ref that should be ignored')).toBeNull()
  })

  it('labels a prop by the most readable name of its ref, with the source ids in a tooltip', async () => {
    const { container } = render(<PropLabel prop="ro:0002200" refs={refs} />)
    expect(screen.getByText('has phenotype')).toBeInTheDocument()
    const icon = container.querySelector('i.icon-info')!
    fireEvent.mouseOver(icon)
    expect(await screen.findByRole('tooltip')).toHaveTextContent('ro:0002200; RO_0002200')
  })

  it('falls back to the raw key in monospace', () => {
    const { container } = render(<PropLabel prop="gwas:pvalue" refs={refs} />)
    const label = screen.getByText('gwas:pvalue')
    expect(label.style.fontFamily).toContain('monospace')
    expect(container.querySelector('i')).toBeNull()
  })

  it('lets a long key or a name without spaces wrap after its separators', () => {
    const { container, unmount } = render(<PropLabel prop="biolink:chemical_to_disease_or_phenotypic_feature_association" refs={refs} />)
    expect(container.textContent).toBe('biolink:chemical_to_disease_or_phenotypic_feature_association')
    expect(container.querySelectorAll('wbr')).toHaveLength(7)
    unmount()

    const urlNamed = new Refs({ 'chemrof:mass': { 'grebi:nodeId': 'chemrof:mass', 'grebi:name': ['https://w3id.org/chemrof/monoisotopic_mass'] } })
    const named = render(<PropLabel prop="chemrof:mass" refs={urlNamed} />)
    expect(named.container.querySelector('b')!.textContent).toBe('https://w3id.org/chemrof/monoisotopic_mass')
    expect(named.container.querySelectorAll('wbr').length).toBeGreaterThan(3)
    named.unmount()

    // a name of several words wraps at its spaces as it is
    const { container: spaced } = render(<PropLabel prop="ro:0002200" refs={refs} />)
    expect(spaced.querySelector('wbr')).toBeNull()
  })
})
