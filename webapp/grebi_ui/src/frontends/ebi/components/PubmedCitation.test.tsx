import { describe, it, expect, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import PubmedCitation from './PubmedCitation'
import { resetPubmedRefs, getRegisteredRefs } from './pubmedRegistry'

beforeEach(() => resetPubmedRefs())

describe('PubmedCitation', () => {
  it('registers its PMID and renders a superscript link to the reference, keeping its children', () => {
    render(<PubmedCitation id="40323307">cited text</PubmedCitation>)
    const link = screen.getByRole('link', { name: '[1]' })
    expect(link).toHaveAttribute('href', '#ref-1')
    expect(link.style.verticalAlign).toBe('super')
    expect(screen.getByText('cited text')).toBeInTheDocument()
    expect(getRegisteredRefs().map((r) => r.pmid)).toEqual(['40323307'])
  })

  it('numbers distinct PMIDs consecutively and reuses the number for a repeat', () => {
    render(
      <>
        <PubmedCitation id="111" />
        <PubmedCitation id="222" />
        <PubmedCitation id="111" />
      </>
    )
    expect(screen.getAllByRole('link').map((a) => a.textContent)).toEqual(['[1]', '[2]', '[1]'])
  })

  it('renders only its children without an id', () => {
    render(<PubmedCitation>plain</PubmedCitation>)
    expect(screen.queryByRole('link')).toBeNull()
    expect(screen.getByText('plain')).toBeInTheDocument()
    expect(getRegisteredRefs()).toEqual([])
  })
})
