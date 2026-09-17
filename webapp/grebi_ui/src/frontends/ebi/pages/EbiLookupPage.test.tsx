import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react'
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom'
import EbiLookupPage, { lookupCsv, splitIdentifiers } from './EbiLookupPage'
import encodeNodeId from '../../../encodeNodeId'

const api = vi.hoisted(() => ({ post: vi.fn() }))

vi.mock('../../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, post: api.post, get: vi.fn(async () => []), getPaginated: vi.fn() }
})

const psoriasis = {
  'grebi:nodeId': 'mondo:0005083',
  'grebi:name': 'psoriasis',
  'grebi:type': ['biolink:Disease'],
  'grebi:datasources': ['OLS.mondo', 'GWAS'],
  'grebi:sourceIds': ['mondo:0005083', 'doid:8893'],
  'grebi:curie': 'mondo:0005083',
}
const answer = {
  results: [
    { id: 'DOID:8893', nodes: [psoriasis] },
    { id: 'nope:1', nodes: [] },
  ],
  notFound: ['nope:1'],
  truncated: false,
}

function LocationDisplay() {
  const location = useLocation()
  return <div data-testid="location">{location.pathname + location.search}</div>
}

function renderPage(url = '/graphs/g1/lookup') {
  return render(
    <MemoryRouter initialEntries={[url]}>
      <Routes>
        <Route path="/graphs/:graph/lookup" element={<EbiLookupPage />} />
        <Route path="/graphs/:graph/nodes/:nodeId" element={<div>node page</div>} />
      </Routes>
      <LocationDisplay />
    </MemoryRouter>
  )
}

/** jsdom's Blob has no text(), so read it the old way */
function readBlob(blob: Blob): Promise<string> {
  return new Promise((resolve) => {
    const reader = new FileReader()
    reader.onload = () => resolve(reader.result as string)
    reader.readAsText(blob)
  })
}

async function lookUp(text: string) {
  fireEvent.change(screen.getByLabelText('Identifiers'), { target: { value: text } })
  fireEvent.click(screen.getByRole('button', { name: /Look up/ }))
  await waitFor(() => expect(api.post).toHaveBeenCalled())
}

let log: ReturnType<typeof vi.spyOn>
beforeEach(() => {
  log = vi.spyOn(console, 'log').mockImplementation(() => {})
  api.post.mockReset()
  api.post.mockResolvedValue(answer)
})
afterEach(() => log.mockRestore())

describe('splitIdentifiers', () => {
  it('takes one identifier per line, or separated by commas, semicolons or tabs, without repeats', () => {
    expect(splitIdentifiers(' mondo:1 \nHGNC:2, hgnc:3;\tmondo:1\n\n')).toEqual(['mondo:1', 'HGNC:2', 'hgnc:3'])
    expect(splitIdentifiers('skin lesion')).toEqual(['skin lesion'])
    expect(splitIdentifiers('')).toEqual([])
  })
})

describe('lookupCsv', () => {
  it('writes a row per identifier and node, quoting what needs it', () => {
    const csv = lookupCsv({
      results: [
        { id: 'a', nodes: [{ ...psoriasis, 'grebi:name': 'psoriasis, "plaque"' }] },
        { id: 'nope', nodes: [] },
      ],
      notFound: ['nope'],
      truncated: false,
    })
    expect(csv.split('\n')).toEqual([
      'identifier,nodeId,name,type,datasources,curie',
      'a,mondo:0005083,"psoriasis, ""plaque""",biolink:Disease,OLS.mondo|GWAS,mondo:0005083',
      'nope,,,,,',
      '',
    ])
  })
})

describe('EbiLookupPage', () => {
  it('has nothing to look up until identifiers are pasted', () => {
    renderPage()
    expect(screen.getByRole('button', { name: 'Look up identifiers' })).toBeDisabled()
    fireEvent.change(screen.getByLabelText('Identifiers'), { target: { value: 'a\nb' } })
    expect(screen.getByRole('button', { name: 'Look up 2 identifiers' })).toBeEnabled()
    expect(api.post).not.toHaveBeenCalled()
  })

  it('posts the identifiers once and lists what each one names', async () => {
    renderPage()
    await lookUp('DOID:8893\nnope:1, DOID:8893')
    expect(api.post).toHaveBeenCalledTimes(1)
    expect(api.post).toHaveBeenCalledWith('api/v1/graphs/g1/lookup', undefined, { ids: ['DOID:8893', 'nope:1'] })
    // the lookup is recorded in the URL, so it can be linked to
    expect(screen.getByTestId('location')).toHaveTextContent('/graphs/g1/lookup?ids=DOID%3A8893%2Cnope%3A1')

    expect(await screen.findByText('1 of 2 identifiers found')).toBeInTheDocument()
    const rows = screen.getAllByRole('row').slice(1)
    expect(rows).toHaveLength(2)
    const link = within(rows[0]).getByRole('link', { name: 'psoriasis' })
    expect(link).toHaveAttribute('href', `/graphs/g1/nodes/${encodeNodeId('mondo:0005083')}`)
    expect(within(rows[0]).getByText('Disease')).toBeInTheDocument()
    // an ontology datasource's tag is the ontology's name
    expect(within(rows[0]).getByText('mondo')).toBeInTheDocument()
    expect(within(rows[0]).getByText('GWAS')).toBeInTheDocument()
    expect(within(rows[1]).getByText('nope:1')).toBeInTheDocument()
    expect(within(rows[1]).getByText('No node has this identifier')).toBeInTheDocument()
  })

  it('shows a type it has no name for as it is', async () => {
    api.post.mockResolvedValue({ results: [{ id: 'a', nodes: [{ ...psoriasis, 'grebi:type': ['owl:Class'] }] }], notFound: [], truncated: false })
    renderPage()
    await lookUp('a')
    const row = (await screen.findAllByRole('row'))[1]
    expect(within(row).getByText('owl:Class')).toBeInTheDocument()
  })

  it('asks for names to be matched too when the box is ticked, and says when matches were left out', async () => {
    api.post.mockResolvedValue({ ...answer, truncated: true })
    renderPage()
    fireEvent.click(screen.getByLabelText('Also match names (slower)'))
    await lookUp('psoriasis')
    expect(api.post).toHaveBeenCalledWith('api/v1/graphs/g1/lookup', { matchNames: 'true' }, { ids: ['psoriasis'] })
    expect(await screen.findByText(/some nodes were left out/)).toBeInTheDocument()
    expect(screen.getByTestId('location')).toHaveTextContent('?ids=psoriasis&matchNames=true')
  })

  it('runs a lookup arrived at by its URL, once', async () => {
    renderPage('/graphs/g1/lookup?ids=DOID:8893,nope:1&matchNames=true')
    expect(screen.getByLabelText('Identifiers')).toHaveValue('DOID:8893\nnope:1')
    expect(screen.getByLabelText('Also match names (slower)')).toBeChecked()
    expect(await screen.findByText('1 of 2 identifiers found')).toBeInTheDocument()
    expect(api.post).toHaveBeenCalledTimes(1)
    expect(api.post).toHaveBeenCalledWith('api/v1/graphs/g1/lookup', { matchNames: 'true' }, { ids: ['DOID:8893', 'nope:1'] })
  })

  it('keeps very long lists out of the URL', async () => {
    renderPage()
    const many = Array.from({ length: 500 }, (_, i) => `identifier:${i}`).join('\n')
    await lookUp(many)
    expect(screen.getByTestId('location')).toHaveTextContent('/graphs/g1/lookup')
    expect(screen.getByTestId('location')).not.toHaveTextContent('ids=')
  })

  it('downloads the answer as a CSV file', async () => {
    const blobs: Blob[] = []
    const createObjectURL = vi.fn((blob: Blob) => { blobs.push(blob); return 'blob:csv' })
    const revokeObjectURL = vi.fn()
    Object.assign(URL, { createObjectURL, revokeObjectURL })
    const click = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {})
    try {
      renderPage()
      await lookUp('DOID:8893\nnope:1')
      fireEvent.click(await screen.findByRole('button', { name: 'Download as CSV' }))
      expect(blobs).toHaveLength(1)
      expect(await readBlob(blobs[0])).toBe(lookupCsv(answer))
      expect(click).toHaveBeenCalledTimes(1)
      expect(revokeObjectURL).toHaveBeenCalledWith('blob:csv')
    } finally {
      click.mockRestore()
    }
  })

  it('shows why the lookup failed', async () => {
    const { ApiError } = await import('../../../app/api')
    api.post.mockRejectedValue(new ApiError(400, 'u', 'A lookup may not include more than 1000 identifiers'))
    renderPage()
    await lookUp('a')
    const alert = await screen.findByRole('alert')
    expect(alert).toHaveTextContent('The identifiers could not be loaded')
    expect(alert).toHaveTextContent('A lookup may not include more than 1000 identifiers (HTTP 400)')
    expect(screen.queryByRole('table')).toBeNull()
  })

  it('links back to the search and shows breadcrumbs for the graph', () => {
    renderPage()
    expect(screen.getByRole('link', { name: 'search' })).toHaveAttribute('href', '/graphs/g1/search')
    const crumbs = within(screen.getByLabelText('breadcrumb'))
    expect(crumbs.getAllByRole('link').map((a) => a.getAttribute('href'))).toEqual(['/', '/graphs', '/graphs/g1/lookup'])
  })
})
