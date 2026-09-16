import { describe, it, expect, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import EbiSearchPage from './EbiSearchPage'

// the search interface is its own component with its own API calls; the page
// only has to read the graph from the route and pass it on
vi.mock('../../../components/SearchInterface', () => ({ default: ({ graph }: any) => <div data-testid="search">search:{graph}</div> }))
vi.mock('../../../app/api', () => ({ get: vi.fn(async () => []), getPaginated: vi.fn(), post: vi.fn() }))

function renderPage(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/graphs/:graph/search" element={<EbiSearchPage />} />
      </Routes>
    </MemoryRouter>
  )
}

describe('EbiSearchPage', () => {
  it('hands the graph from the route to the search interface', () => {
    renderPage('/graphs/g1/search?q=psoriasis')
    expect(screen.getByTestId('search')).toHaveTextContent('search:g1')
  })

  it('shows breadcrumbs for the graph with a graph picker', () => {
    renderPage('/graphs/g1/search')
    const crumbs = within(screen.getByLabelText('breadcrumb'))
    expect(crumbs.getAllByRole('link').map((a) => a.getAttribute('href'))).toEqual(['/', '/graphs', '/graphs/g1/search'])
    expect(crumbs.getByRole('link', { name: 'Search' })).toBeInTheDocument()
    expect(crumbs.getByRole('combobox')).toBeInTheDocument()
  })
})
