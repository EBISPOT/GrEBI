import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import EbiBreadcrumbsBar from './EbiBreadcrumbsBar'

vi.mock('../../app/api', () => ({ get: vi.fn() }))
import { get } from '../../app/api'
const mockedGet = vi.mocked(get)

function LocationDisplay() {
  const loc = useLocation()
  return <div data-testid="location">{loc.pathname + loc.search + loc.hash}</div>
}

function renderBar(path: string, props: React.ComponentProps<typeof EbiBreadcrumbsBar>) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <EbiBreadcrumbsBar {...props} />
      <LocationDisplay />
    </MemoryRouter>
  )
}

const entries = [
  { url: '/graphs', label: 'Graphs' },
  { url: '/graphs/g1/nodes', label: 'Nodes' },
  { url: '/graphs/g1/nodes/x', label: <code>x</code> },
]

beforeEach(() => {
  mockedGet.mockReset()
  mockedGet.mockImplementation(async (path: string) => {
    if (path === 'api/v1/graphs') return ['g1', 'g2']
    if (path === 'api/v1/stats') return {}
    throw new Error('unexpected GET ' + path)
  })
})

describe('EbiBreadcrumbsBar', () => {
  it('renders a home link followed by the entries, in order', () => {
    renderBar('/graphs/g1/nodes/x', { entries })
    const links = screen.getAllByRole('link')
    expect(links.map((a) => a.getAttribute('href'))).toEqual(['/', '/graphs', '/graphs/g1/nodes', '/graphs/g1/nodes/x'])
    expect(links[3].querySelector('code')).toHaveTextContent('x')
    // no graph: no picker, no API calls
    expect(screen.queryByRole('combobox')).toBeNull()
    expect(mockedGet).not.toHaveBeenCalled()
  })

  it('shows the graph picker between the first entry and the rest when a graph is given', async () => {
    renderBar('/graphs/g1/nodes/x', { graph: 'g1', entries })
    const picker = screen.getByRole('combobox')
    // the picker can only display the value once the graph list has arrived
    await waitFor(() => expect(picker).toHaveTextContent('g1'))
    expect(mockedGet).toHaveBeenCalledWith('api/v1/graphs')
    // the picker sits after the "Graphs" link and before the remaining crumbs
    const items = Array.from(screen.getByLabelText('breadcrumb').querySelectorAll('li.MuiBreadcrumbs-li'))
    expect(items.findIndex((li) => li.contains(picker))).toBe(2)
  })

  it('picking another graph navigates to the same page for that graph, keeping query and hash', async () => {
    renderBar('/graphs/g1/nodes/x?tab=edges_in#top', { graph: 'g1', entries })
    fireEvent.mouseDown(screen.getByRole('combobox'))
    fireEvent.click(await screen.findByRole('option', { name: 'g2' }))
    await waitFor(() => expect(screen.getByTestId('location')).toHaveTextContent('/graphs/g2/nodes/x?tab=edges_in#top'))
  })
})
