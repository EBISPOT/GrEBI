import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import EbiFooter from './EbiFooter'

describe('EbiFooter', () => {
  it('links to the repository and licensing pages from the environment, opening in a new tab', () => {
    process.env.REACT_APP_SPOT_GREBI_REPO = 'https://github.com/EBISPOT/GrEBI'
    process.env.REACT_APP_EBI_LICENSING = 'https://www.ebi.ac.uk/licencing'
    render(<EbiFooter />)

    const github = screen.getByTitle('GitHub')
    expect(github).toHaveAttribute('href', 'https://github.com/EBISPOT/GrEBI')
    expect(github).toHaveAttribute('target', '_blank')
    expect(github).toHaveAttribute('rel', 'noopener noreferrer')

    const licensing = screen.getByRole('link', { name: 'Licensing' })
    expect(licensing).toHaveAttribute('href', 'https://www.ebi.ac.uk/licencing')
    expect(licensing).toHaveAttribute('target', '_blank')
  })

  it('shows the EMBL-EBI copyright line', () => {
    render(<EbiFooter />)
    // the source uses a non-breaking space, which the text matcher normalises
    expect(screen.getByText(/EMBL-EBI 2024/)).toBeInTheDocument()
    expect(screen.getByText('Follow us')).toBeInTheDocument()
  })
})
