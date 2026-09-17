import { describe, it, expect, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import EbiDownloadsPage from './EbiDownloadsPage'

vi.mock('../../../app/api', () => ({ get: vi.fn(async () => []), getPaginated: vi.fn(), post: vi.fn() }))

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/graphs/g1/downloads']}>
      <Routes>
        <Route path="/graphs/:graph/downloads" element={<EbiDownloadsPage />} />
      </Routes>
    </MemoryRouter>
  )
}

describe('EbiDownloadsPage', () => {
  it('points at the FTP area in a new tab', () => {
    renderPage()
    expect(screen.getByText('Downloading Knowledge Graph Exports')).toBeInTheDocument()
    const ftp = screen.getByRole('link', { name: 'https://ftp.ebi.ac.uk/pub/databases/spot/kg/' })
    expect(ftp).toHaveAttribute('href', 'https://ftp.ebi.ac.uk/pub/databases/spot/kg/')
    expect(ftp).toHaveAttribute('target', '_blank')
    expect(ftp).toHaveAttribute('rel', 'noopener noreferrer')
  })

  it('lists the files of the graph in the latest release', () => {
    renderPage()
    const rows = screen.getAllByRole('row').slice(1)
    expect(rows.map((r) => within(r).getByRole('link').textContent)).toEqual([
      'g1_neo4j.tar.xz', 'g1_metadata.json', 'query_results/', 'postgres.tar.xz', 'release.tar.xz',
    ])
    expect(within(rows[0]).getByRole('link')).toHaveAttribute('href', 'https://ftp.ebi.ac.uk/pub/databases/spot/kg/latest/g1_neo4j.tar.xz')
    expect(screen.getByRole('link', { name: 'latest/' })).toHaveAttribute('href', 'https://ftp.ebi.ac.uk/pub/databases/spot/kg/latest/')
  })

  it('shows breadcrumbs for the graph in the route', () => {
    renderPage()
    const crumbs = within(screen.getByLabelText('breadcrumb'))
    expect(crumbs.getAllByRole('link').map((a) => a.getAttribute('href'))).toEqual(['/', '/graphs', '/graphs/g1/downloads'])
    expect(crumbs.getByRole('combobox')).toBeInTheDocument()
  })
})
