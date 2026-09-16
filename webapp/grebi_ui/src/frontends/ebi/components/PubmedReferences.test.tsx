import { describe, it, expect, beforeEach } from 'vitest'
import { render, screen, act } from '@testing-library/react'
import PubmedReferences from './PubmedReferences'
import { registerPmid, resetPubmedRefs } from './pubmedRegistry'

beforeEach(() => resetPubmedRefs())

describe('PubmedReferences', () => {
  it('renders nothing while no citation has been registered', () => {
    const { container } = render(<PubmedReferences />)
    expect(container).toBeEmptyDOMElement()
  })

  it('formats cached references and links them to PubMed', () => {
    registerPmid('40323307')
    render(<PubmedReferences />)
    const item = screen.getByRole('listitem')
    expect(item).toHaveAttribute('id', 'ref-1')
    expect(item).toHaveTextContent(
      '[1]McLaughlin J, Lagrimas J, Iqbal H, Parkinson H, Harmse H. OLS4: a new Ontology Lookup Service for a growing interdisciplinary knowledge ecosystem. Bioinformatics (2025) 41(5). doi: 10.1093/bioinformatics/btaf279'
    )
    expect(item.querySelector('em')).toHaveTextContent('Bioinformatics')
    const link = screen.getByRole('link')
    expect(link).toHaveAttribute('href', 'https://pubmed.ncbi.nlm.nih.gov/40323307/')
    expect(link).toHaveAttribute('target', '_blank')
  })

  it('falls back to the bare PMID for references that are not in the cache', () => {
    registerPmid('123456')
    render(<PubmedReferences />)
    expect(screen.getByRole('listitem')).toHaveTextContent('[1]PMID: 123456')
  })

  it('follows registrations made after it mounted, and empties on reset', () => {
    const { container } = render(<PubmedReferences />)
    act(() => { registerPmid('111') })
    act(() => { registerPmid('222') })
    expect(screen.getAllByRole('listitem').map((li) => li.id)).toEqual(['ref-1', 'ref-2'])

    act(() => resetPubmedRefs())
    expect(container).toBeEmptyDOMElement()
  })
})
