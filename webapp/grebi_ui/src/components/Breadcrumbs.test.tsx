import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Breadcrumbs from './Breadcrumbs'

// GraphPicker fetches from the API; a stub keeps this test about the crumbs.
vi.mock('./GraphPicker', () => ({ default: ({ graph }: { graph: string }) => <span data-testid="graph-picker">{graph}</span> }))

const entries = [
  { url: '/graphs', label: 'Graphs' },
  { url: '/graphs/g/nodes', label: <em>Nodes</em> },
  { url: '/graphs/g/nodes/x', label: 'psoriasis' },
]

const renderCrumbs = (props: Partial<React.ComponentProps<typeof Breadcrumbs>> = {}) =>
  render(<MemoryRouter><Breadcrumbs entries={entries} {...props} /></MemoryRouter>)

describe('Breadcrumbs', () => {
  it('starts with a home link followed by a link per entry, in order', () => {
    renderCrumbs()
    const links = screen.getAllByRole('link')
    expect(links.map(l => l.getAttribute('href'))).toEqual(['/', '/graphs', '/graphs/g/nodes', '/graphs/g/nodes/x'])
    expect(links[2]).toContainHTML('<em>Nodes</em>')
    expect(links[3]).toHaveTextContent('psoriasis')
    expect(screen.queryByTestId('graph-picker')).toBeNull()
  })

  it('places the graph picker after the first entry when a graph and setter are given', () => {
    renderCrumbs({ graph: 'test_gwas', setGraph: vi.fn() })
    const picker = screen.getByTestId('graph-picker')
    expect(picker).toHaveTextContent('test_gwas')
    const items = Array.from(screen.getByLabelText('breadcrumb').querySelectorAll('li.MuiBreadcrumbs-li'))
    expect(items.map(li => li.textContent)).toEqual(['', 'Graphs', 'test_gwas', 'Nodes', 'psoriasis'])
  })

  it('omits the picker when only one of graph and setGraph is given', () => {
    renderCrumbs({ graph: 'test_gwas' })
    expect(screen.queryByTestId('graph-picker')).toBeNull()
  })

  it('shows only the home link with no entries', () => {
    renderCrumbs({ entries: [] })
    expect(screen.getAllByRole('link')).toHaveLength(1)
  })
})
