import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import EbiViewsPage from './EbiViewsPage'

process.env.PUBLIC_URL = '/'

vi.mock('../../../app/api', () => ({ get: vi.fn(async () => ['g1']), getPaginated: vi.fn(), post: vi.fn() }))

beforeEach(() => sessionStorage.clear())

describe('EbiViewsPage', () => {
  it('renders the Analyses heading under the site header', async () => {
    render(<MemoryRouter><EbiViewsPage /></MemoryRouter>)
    expect(screen.getByText('Analyses')).toHaveClass('text-2xl')
    expect(screen.getByRole('menuitem', { name: 'Explore' })).toBeInTheDocument()
    await waitFor(() => expect(document.title).toBe('Analyses - GrEBI'))
  })

  it('does not highlight any nav section, since "analyses" is not one', () => {
    render(<MemoryRouter><EbiViewsPage /></MemoryRouter>)
    const active = screen.getAllByRole('menuitem').filter((li) => li.className.includes('bg-opacity-30'))
    expect(active).toEqual([])
  })
})
