import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import EbiHeader from './EbiHeader'

process.env.PUBLIC_URL = '/'

vi.mock('../../app/api', () => ({ get: vi.fn() }))
import { get } from '../../app/api'
const mockedGet = vi.mocked(get)

function LocationDisplay() {
  const loc = useLocation()
  return <div data-testid="location">{loc.pathname + loc.search + loc.hash}</div>
}

function renderHeader(path: string, props: React.ComponentProps<typeof EbiHeader>) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <EbiHeader {...props} />
      <LocationDisplay />
    </MemoryRouter>
  )
}

const menuItem = (name: string) => screen.getByRole('menuitem', { name })
const isActive = (name: string) => menuItem(name).className.includes('bg-opacity-30')
const hrefOf = (name: string) => menuItem(name).closest('a')!.getAttribute('href')

beforeEach(() => {
  sessionStorage.clear()
  mockedGet.mockReset()
  mockedGet.mockImplementation(async (path: string) => {
    if (path === 'api/v1/graphs') return ['g1', 'g2']
    if (path === 'api/v1/stats') return { g1: { num_nodes: 10, num_edges: 20 } }
    throw new Error('unexpected GET ' + path)
  })
})

describe('EbiHeader', () => {
  it.each([
    ['home', 'Explore'],
    ['explore', 'Explore'],
    ['search', 'Explore'],
    ['graphs', 'Graphs'],
    ['about', 'Graphs'],
    ['queries', 'Queries'],
    ['tables', 'Tables'],
    ['downloads', 'Downloads'],
  ])('section %s highlights %s', (section, active) => {
    renderHeader('/', { section })
    expect(['Explore', 'Graphs', 'Queries', 'Tables', 'Downloads'].filter(isActive)).toEqual([active])
  })

  it('scopes links to the given graph, otherwise to the remembered graph', () => {
    const { unmount } = renderHeader('/graphs/g1/queries', { section: 'queries', graph: 'g1' })
    expect(hrefOf('Queries')).toBe('/graphs/g1/queries')
    expect(hrefOf('Tables')).toBe('/graphs/g1/tables')
    expect(hrefOf('Downloads')).toBe('/graphs/g1/downloads')
    unmount()

    sessionStorage.setItem('grebi_last_graph', 'g2')
    renderHeader('/', { section: 'home' })
    expect(hrefOf('Queries')).toBe('/graphs/g2/queries')
  })

  it('only shows the breadcrumbs bar when asked, with the given entries', () => {
    const { unmount } = renderHeader('/', { section: 'home' })
    expect(screen.queryByLabelText('breadcrumb')).toBeNull()
    unmount()

    renderHeader('/graphs/g1/queries', {
      section: 'queries',
      showBreadcrumbsBar: true,
      breadcrumbs: [{ url: '/graphs', label: 'Graphs' }, { url: '/graphs/g1/queries', label: 'Queries' }],
    })
    // the nav menu has links with the same names, so look inside the breadcrumb trail
    const crumbs = within(screen.getByLabelText('breadcrumb'))
    expect(crumbs.getAllByRole('link').map((a) => a.getAttribute('href'))).toEqual(['/', '/graphs', '/graphs/g1/queries'])
    expect(crumbs.getByRole('link', { name: 'Graphs' })).toBeInTheDocument()
    expect(crumbs.getByRole('link', { name: 'Queries' })).toBeInTheDocument()
  })

  it('switching graph in the breadcrumb picker swaps the graph segment of the current URL', async () => {
    renderHeader('/graphs/g1/queries?x=1#frag', {
      section: 'queries',
      graph: 'g1',
      showBreadcrumbsBar: true,
      breadcrumbs: [{ url: '/graphs', label: 'Graphs' }],
    })
    fireEvent.mouseDown(screen.getByRole('combobox'))
    fireEvent.click(await screen.findByRole('option', { name: /^g2/ }))
    await waitFor(() => expect(screen.getByTestId('location')).toHaveTextContent('/graphs/g2/queries?x=1#frag'))
  })

  it('sets the document title from the capitalised section', async () => {
    renderHeader('/', { section: 'downloads' })
    await waitFor(() => expect(document.title).toBe('Downloads - GrEBI'))
  })
})
