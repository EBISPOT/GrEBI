import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import EbiLayout from './EbiLayout'

process.env.PUBLIC_URL = '/'

vi.mock('../../app/api', () => ({ get: vi.fn() }))
import { get } from '../../app/api'
const mockedGet = vi.mocked(get)

function renderLayout(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route element={<EbiLayout />}>
          <Route path="*" element={<div>page body</div>} />
        </Route>
      </Routes>
    </MemoryRouter>
  )
}

const menuItem = (name: string) => screen.getByRole('menuitem', { name })
const isActive = (name: string) => menuItem(name).className.includes('bg-opacity-30')
const hrefOf = (name: string) => menuItem(name).closest('a')!.getAttribute('href')

beforeEach(() => {
  sessionStorage.clear()
  mockedGet.mockReset()
  mockedGet.mockResolvedValue(['fallback_graph'])
})

describe('EbiLayout', () => {
  it('renders the navigation around the routed page', () => {
    renderLayout('/')
    expect(screen.getByText('page body')).toBeInTheDocument()
    expect(screen.getAllByRole('menuitem').map((li) => li.textContent)).toEqual([
      'Explore', 'Graphs', 'Queries', 'Tables', 'Downloads', 'Docs', 'GitHub',
    ])
    expect(screen.getByAltText('GrEBI logo').closest('a')).toHaveAttribute('href', '/')
    expect(menuItem('GitHub').closest('a')).toHaveAttribute('href', 'https://github.com/EBISPOT/GrEBI')
  })

  it.each([
    ['/', 'Explore'],
    ['/graphs', 'Graphs'],
    ['/graphs/g1', 'Graphs'],
    ['/graphs/g1/queries/x', 'Queries'],
    ['/graphs/g1/tables', 'Tables'],
    ['/graphs/g1/downloads', 'Downloads'],
    ['/docs/anything', 'Docs'],
    ['/graphs/g1/search?q=x', 'Explore'],
    ['/graphs/g1/nodes/abc', 'Explore'],
  ])('%s highlights %s', (path, active) => {
    renderLayout(path)
    const items = ['Explore', 'Graphs', 'Queries', 'Tables', 'Downloads', 'Docs']
    expect(items.filter(isActive)).toEqual([active])
  })

  it('scopes the queries, tables and downloads links to the graph in the URL', () => {
    renderLayout('/graphs/g1/nodes/abc')
    expect(hrefOf('Queries')).toBe('/graphs/g1/queries')
    expect(hrefOf('Tables')).toBe('/graphs/g1/tables')
    expect(hrefOf('Downloads')).toBe('/graphs/g1/downloads')
    expect(sessionStorage.getItem('grebi_last_graph')).toBe('g1')
    expect(mockedGet).not.toHaveBeenCalled()
  })

  it('falls back to the remembered graph, or the first graph from the API, when the URL has none', async () => {
    sessionStorage.setItem('grebi_last_graph', 'remembered')
    const { unmount } = renderLayout('/docs')
    expect(hrefOf('Queries')).toBe('/graphs/remembered/queries')
    expect(mockedGet).not.toHaveBeenCalled()
    unmount()

    sessionStorage.clear()
    renderLayout('/docs')
    expect(hrefOf('Queries')).toBe('/graphs')
    await waitFor(() => expect(hrefOf('Queries')).toBe('/graphs/fallback_graph/queries'))
    expect(mockedGet).toHaveBeenCalledWith('api/v1/graphs')
  })

  it('sets the document title from the active section', async () => {
    renderLayout('/graphs/g1/queries')
    await waitFor(() => expect(document.title).toBe('Queries - GrEBI'))
  })
})
