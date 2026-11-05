import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { MemoryRouter, Routes, Route, Link, useLocation } from 'react-router-dom'
import WidgetLayout from './WidgetLayout'
import { useEmbedded } from '../../app/embedded'
import { configureInstance } from '../../app/instance'

function PageA() {
  return (
    <div>
      Page A <Link to="/b?x=1#top">to B</Link>
      <a href="https://europepmc.org/abstract/MED/1">PubMed</a>
    </div>
  )
}
function PageB() {
  const loc = useLocation()
  return <div>Page B at {loc.pathname + loc.search + loc.hash} <Link to="/c">to C</Link></div>
}
function Probe() {
  return <div data-testid="embedded">{String(useEmbedded())}</div>
}

function renderAt(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route element={<WidgetLayout />}>
          <Route path="/a" element={<><PageA /><Probe /></>} />
          <Route path="/b" element={<PageB />} />
          <Route path="/c" element={<div>Page C</div>} />
        </Route>
      </Routes>
    </MemoryRouter>
  )
}

afterEach(() => {
  configureInstance({ siteUrl: undefined })
  vi.restoreAllMocks()
})

describe('WidgetLayout', () => {
  it('shows the page inside the embedded context, under a bar linking to the same page on GrEBI', () => {
    configureInstance({ siteUrl: 'https://kg.example/spot/kg/' })
    renderAt('/a')
    expect(screen.getByText('Page A')).toBeInTheDocument()
    expect(screen.getByTestId('embedded')).toHaveTextContent('true')
    expect(screen.getByRole('link', { name: 'Open in GrEBI ↗' })).toHaveAttribute('href', 'https://kg.example/spot/kg/a')
    expect(screen.getByRole('link', { name: 'GrEBI' })).toHaveAttribute('href', 'https://kg.example/spot/kg/')
    expect(screen.getByAltText('GrEBI')).toHaveAttribute('src', 'https://kg.example/spot/kg/logo.svg')
    expect(screen.queryByRole('button', { name: '← Back' })).toBeNull()
  })

  it('follows links inside the widget, offering a way back to where it started', async () => {
    renderAt('/a')
    fireEvent.click(screen.getByRole('link', { name: 'to B' }))
    expect(await screen.findByText('Page B at /b?x=1#top')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Open in GrEBI ↗' })).toHaveAttribute('href', '/b?x=1#top')
    fireEvent.click(screen.getByRole('link', { name: 'to C' }))
    expect(await screen.findByText('Page C')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: '← Back' }))
    expect(await screen.findByText(/Page B at/)).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: '← Back' }))
    expect(await screen.findByText('Page A')).toBeInTheDocument()
    await waitFor(() => expect(screen.queryByRole('button', { name: '← Back' })).toBeNull())
  })

  it('opens a modified click on a link as the page on GrEBI, in a new tab', () => {
    configureInstance({ siteUrl: 'https://kg.example/spot/kg/' })
    const open = vi.spyOn(window, 'open').mockImplementation(() => null)
    renderAt('/a')
    fireEvent.click(screen.getByRole('link', { name: 'to B' }), { ctrlKey: true })
    expect(open).toHaveBeenCalledWith('https://kg.example/spot/kg/b?x=1#top', '_blank', 'noopener')
    fireEvent(screen.getByRole('link', { name: 'to B' }), new MouseEvent('auxclick', { button: 1, bubbles: true, cancelable: true }))
    expect(open).toHaveBeenCalledTimes(2)
    // still on A: neither was followed inside the widget
    expect(screen.getByText('Page A')).toBeInTheDocument()
    // an external link is the browser's business
    fireEvent.click(screen.getByRole('link', { name: 'PubMed' }), { ctrlKey: true })
    expect(open).toHaveBeenCalledTimes(2)
  })
})
