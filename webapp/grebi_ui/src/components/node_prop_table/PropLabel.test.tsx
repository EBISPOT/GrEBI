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
})
