import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import addLinksToText from './addLinksToText'

describe('addLinksToText', () => {
  it('turns known type CURIEs into badges and leaves other CURIEs as text', () => {
    render(<div>{addLinksToText('a gwas:SNP near mondo:0005083', 'ebi_monarch_xspecies')}</div>)
    expect(screen.getByTitle('SNP')).toHaveTextContent('SNP')
    expect(screen.getByText(/mondo:0005083/)).toBeInTheDocument()
    expect(screen.queryByTitle('Disease')).toBeNull()
  })

  it('returns the text untouched without a graph', () => {
    expect(addLinksToText('a gwas:SNP', undefined)).toBe('a gwas:SNP')
  })
})
