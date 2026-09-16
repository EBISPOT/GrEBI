import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import DocsPage from './DocsPage'

process.env.PUBLIC_URL = '/'

const manifest = {
  sidebar: [
    { title: 'Intro', anchor: 'intro', children: [{ title: 'Getting started', anchor: 'getting-started' }] },
    { title: 'Queries', anchor: 'queries' },
  ],
  pages: [
    { title: 'Intro', anchor: 'intro', content: '# Intro\n\nWelcome to the docs\n\n## Getting started\n\nInstall it' },
    { title: 'Queries', anchor: 'queries', content: '# Queries\n\nQuery docs' },
  ],
  images: {},
}

let fetchMock: ReturnType<typeof vi.fn>

beforeEach(() => {
  fetchMock = vi.fn(async () => ({ ok: true, status: 200, json: async () => manifest }))
  vi.stubGlobal('fetch', fetchMock)
  // jsdom has neither of these
  vi.stubGlobal('IntersectionObserver', class {
    observe = vi.fn()
    unobserve = vi.fn()
    disconnect = vi.fn()
  })
  Element.prototype.scrollTo = vi.fn()
})

afterEach(() => {
  vi.unstubAllGlobals()
  window.history.replaceState(null, '', '/')
})

function renderDocs(path = '/docs') {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/docs/*" element={<DocsPage />} />
        <Route path="/docs" element={<DocsPage />} />
      </Routes>
    </MemoryRouter>
  )
}

describe('DocsPage', () => {
  it('fetches the manifest and shows the first page with a numbered sidebar', async () => {
    renderDocs()
    expect(screen.getByText('Loading documentation…')).toBeInTheDocument()

    expect(await screen.findByText('Welcome to the docs')).toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledWith('/docs-manifest.json')
    expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent('1Intro')
    expect(screen.getByRole('heading', { level: 2 })).toHaveTextContent('1.1Getting started')
    // sidebar (the first nav; "See also" is another): top level entries numbered
    const sidebar = screen.getAllByRole('navigation')[0]
    expect(sidebar).toHaveTextContent('1Intro')
    // nothing is active yet, so the Intro branch stays collapsed
    expect(sidebar).not.toHaveTextContent('Getting started')
    expect(sidebar).toHaveTextContent('2Queries')
    expect(screen.getByText(/still in beta testing/)).toBeInTheDocument()
  })

  it('reports a failed manifest load', async () => {
    fetchMock.mockResolvedValue({ ok: false, status: 404 })
    renderDocs()
    expect(await screen.findByText('Failed to load documentation: HTTP 404')).toBeInTheDocument()
  })

  it('opens the page owning the anchor in the URL hash', async () => {
    renderDocs('/docs#queries')
    expect(await screen.findByText('Query docs')).toBeInTheDocument()
    expect(screen.queryByText('Welcome to the docs')).toBeNull()
    expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent('2Queries')
  })

  it('navigates between pages from the sidebar and the "See also" list, updating the hash', async () => {
    renderDocs()
    await screen.findByText('Welcome to the docs')

    fireEvent.click(screen.getByText('Queries', { selector: 'nav span' }))
    expect(await screen.findByText('Query docs')).toBeInTheDocument()
    expect(window.location.hash).toBe('#queries')

    const seeAlso = screen.getByRole('heading', { name: 'See also' }).parentElement!
    expect(seeAlso).toHaveTextContent('Intro')
    expect(seeAlso).not.toHaveTextContent('Queries')
    fireEvent.click(screen.getByRole('link', { name: 'Intro' }))
    expect(await screen.findByText('Welcome to the docs')).toBeInTheDocument()
    expect(window.location.hash).toBe('#intro')
  })

  it('highlights the sidebar entry for a sub-heading and keeps its page open', async () => {
    renderDocs('/docs#getting-started')
    await screen.findByText('Welcome to the docs')
    const entry = screen.getByText('Getting started', { selector: 'nav span' }).closest('div')!
    await waitFor(() => expect(entry.className).toContain('bg-blue-100'))
  })
})
