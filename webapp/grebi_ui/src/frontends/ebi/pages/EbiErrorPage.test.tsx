import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import EbiErrorPage from './EbiErrorPage'

process.env.PUBLIC_URL = '/'

vi.mock('../../../app/api', () => ({ get: vi.fn(async () => ['g1']), getPaginated: vi.fn(), post: vi.fn() }))

beforeEach(() => sessionStorage.clear())

describe('EbiErrorPage', () => {
  it('explains the page was not found and links home', () => {
    render(<MemoryRouter initialEntries={['/nope']}><EbiErrorPage /></MemoryRouter>)
    expect(screen.getByText('Ooops! The requested page cannot be found.')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Return to home' })).toHaveAttribute('href', '/')
    expect(screen.getByAltText(/person using microscope/)).toHaveAttribute('src', '/not-found.jpg')
    expect(screen.getByRole('menuitem', { name: 'Explore' })).toBeInTheDocument()
  })

  it('shows the message passed in the navigation state', () => {
    const { unmount } = render(<MemoryRouter initialEntries={['/error']}><EbiErrorPage /></MemoryRouter>)
    expect(screen.queryByText('Node not found')).toBeNull()
    unmount()

    render(
      <MemoryRouter initialEntries={[{ pathname: '/error', state: { message: 'Node not found' } }]}>
        <EbiErrorPage />
      </MemoryRouter>
    )
    expect(screen.getByText('Node not found')).toBeInTheDocument()
  })

  it('sets the document title', () => {
    render(<MemoryRouter><EbiErrorPage /></MemoryRouter>)
    // NOTE: current behaviour, looks like a bug: the title names the Ontology
    // Lookup Service (copied from OLS) rather than GrEBI.
    expect(document.title).toBe('Ontology Lookup Service (OLS)')
  })
})
