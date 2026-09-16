import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, within, fireEvent, waitFor, act, cleanup } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import ResultsTable from './ResultsTable'
import { Page } from '../../app/api'

const api = vi.hoisted(() => ({ getPaginated: vi.fn() }))

vi.mock('../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, getPaginated: api.getPaginated }
})

const glucose = { 'grebi:nodeId': 'chebi:17234', 'grebi:name': ['glucose'] }
const rows = [
  { chemical: glucose, study: { 'grebi:nodeId': 'MTBLS1', 'grebi:name': ['Study one'] } },
  { chemical: glucose, study: { 'grebi:nodeId': 'MTBLS2', 'grebi:name': ['Study two'] } },
]
const columns = [
  { column_id: 'chemical', column_type: 'GraphNodeId' },
  { column_id: 'study', column_type: 'GraphNodeId' },
]
// the API serialises the facet breakdown as a plain object, not a Map
const facets = { chemical: { glucose: 2, 'D-glucose': 1 } }

const page = (elements: any[] = rows, facetFieldsToCounts: any = facets) =>
  new Page(0, elements.length, 1, elements.length, elements, facetFieldsToCounts)

const params = { chemical_id: 'chebi:17234' }
const lastParams = (): URLSearchParams => api.getPaginated.mock.calls.at(-1)![1]

type Props = Parameters<typeof ResultsTable>[0]

function renderTable(props: Partial<Props> = {}) {
  return render(
    <MemoryRouter>
      <ResultsTable graph="g" queryId="chebi_to_metabolights" params={params} resultColumns={columns} {...props} />
    </MemoryRouter>
  )
}

const facetOption = (value: string) => screen.getByTitle(value)
const facetCheckbox = (value: string) => within(facetOption(value)).getByRole('checkbox')

beforeEach(() => {
  api.getPaginated.mockReset()
  api.getPaginated.mockImplementation(async () => page())
})

describe('ResultsTable narrowing, sorting and export', () => {
  it('shows the facet breakdown and narrows the query when a value is ticked', async () => {
    renderTable()
    await screen.findAllByRole('link', { name: 'glucose' })
    expect(lastParams().toString()).toBe('chemical_id=chebi%3A17234&page=0&size=10&resolve=false')

    expect(screen.getByText('Filter results')).toBeInTheDocument()
    expect(within(facetOption('glucose')).getByText('2')).toBeInTheDocument()
    expect(within(facetOption('D-glucose')).getByText('1')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Clear all' })).toBeNull()

    fireEvent.click(facetCheckbox('glucose'))
    await waitFor(() => expect(api.getPaginated).toHaveBeenCalledTimes(2))
    expect(lastParams().getAll('chemical')).toEqual(['glucose'])
    expect(lastParams().get('page')).toBe('0')
    expect(facetCheckbox('glucose')).toBeChecked()

    fireEvent.click(screen.getByRole('button', { name: 'Clear all' }))
    await waitFor(() => expect(api.getPaginated).toHaveBeenCalledTimes(3))
    expect(lastParams().has('chemical')).toBe(false)
    expect(facetCheckbox('glucose')).not.toBeChecked()
  })

  it('keeps a ticked value visible, without a count, when it drops out of the breakdown', async () => {
    renderTable()
    await screen.findAllByRole('link', { name: 'glucose' })

    api.getPaginated.mockImplementation(async () => page(rows, { chemical: { glucose: 2 } }))
    fireEvent.click(facetCheckbox('D-glucose'))
    await waitFor(() => expect(api.getPaginated).toHaveBeenCalledTimes(2))

    expect(facetCheckbox('D-glucose')).toBeChecked()
    expect(facetOption('D-glucose')).toHaveTextContent(/^D-glucose$/)
    expect(within(facetOption('glucose')).getByText('2')).toBeInTheDocument()
  })

  it('debounces the free-text filter and applies it immediately on Enter', async () => {
    vi.useFakeTimers()
    try {
      renderTable({ materialised: true })
      await act(async () => {})
      const input = screen.getByPlaceholderText('Filter results…')
      expect(api.getPaginated).toHaveBeenCalledTimes(1)

      fireEvent.change(input, { target: { value: 'gluc' } })
      act(() => {
        vi.advanceTimersByTime(349)
      })
      expect(api.getPaginated).toHaveBeenCalledTimes(1)
      act(() => {
        vi.advanceTimersByTime(1)
      })
      expect(api.getPaginated).toHaveBeenCalledTimes(2)
      expect(lastParams().get('q')).toBe('gluc')

      fireEvent.change(input, { target: { value: ' glucose ' } })
      fireEvent.keyDown(input, { key: 'Enter' })
      expect(api.getPaginated).toHaveBeenCalledTimes(3)
      expect(lastParams().get('q')).toBe('glucose')
      // the pending debounce for the same text changes nothing
      act(() => {
        vi.advanceTimersByTime(350)
      })
      expect(api.getPaginated).toHaveBeenCalledTimes(3)

      // the export carries the same narrowing as the table
      expect(screen.getByRole('link', { name: /All Results as CSV/ })).toHaveAttribute(
        'href',
        'http://localhost:3000/api/v1/graphs/g/query/chebi_to_metabolights.csv?chemical_id=chebi%3A17234&q=glucose'
      )
    } finally {
      cleanup()
      vi.clearAllTimers()
      vi.useRealTimers()
    }
  })

  it('sorting a column sends sortBy/sortDir and clicking again flips the direction', async () => {
    renderTable()
    await screen.findAllByRole('link', { name: 'glucose' })

    fireEvent.click(screen.getAllByTestId('SwapVertIcon')[0])
    await waitFor(() => expect(api.getPaginated).toHaveBeenCalledTimes(2))
    expect(lastParams().get('sortBy')).toBe('chemical')
    expect(lastParams().get('sortDir')).toBe('asc')

    fireEvent.click(screen.getByTestId('ArrowDownwardIcon'))
    await waitFor(() => expect(api.getPaginated).toHaveBeenCalledTimes(3))
    expect(lastParams().get('sortDir')).toBe('desc')

    fireEvent.click(screen.getByTestId('ArrowUpwardIcon'))
    await waitFor(() => expect(api.getPaginated).toHaveBeenCalledTimes(4))
    expect(lastParams().get('sortDir')).toBe('asc')

    // the export is unsorted: it always returns every row
    expect(screen.getByRole('link', { name: /All Results as CSV/ }).getAttribute('href')).not.toContain('sortBy')
  })

  it('waits for parameters and hides the sidebar when there is nothing to narrow by', async () => {
    api.getPaginated.mockImplementation(async () => page([], {}))
    const { rerender } = render(
      <MemoryRouter>
        <ResultsTable graph="g" queryId="q" params={undefined} resultColumns={columns} />
      </MemoryRouter>
    )
    expect(api.getPaginated).not.toHaveBeenCalled()
    expect(screen.queryByText('Filter results')).toBeNull()

    rerender(
      <MemoryRouter>
        <ResultsTable graph="g" queryId="q" params={params} resultColumns={columns} />
      </MemoryRouter>
    )
    await waitFor(() => expect(api.getPaginated).toHaveBeenCalledTimes(1))
    await waitFor(() => expect(screen.queryByText('Loading results...')).toBeNull())
    expect(screen.queryByText('Filter results')).toBeNull()
    expect(screen.queryByRole('link', { name: 'glucose' })).toBeNull()
    // NOTE: current behaviour, looks like a bug: the "No results found" placeholder is passed to
    // DataTable, which only uses it for a filter box ResultsTable never enables, so an empty
    // result set is an empty table with no message (ResultsTable.tsx:313, DataTable.tsx:127)
    expect(screen.queryByText('No results found')).toBeNull()
  })

  it('ignores a slow earlier response that arrives after a newer one', async () => {
    let resolveFirst!: (p: Page<any>) => void
    api.getPaginated.mockImplementationOnce(() => new Promise<Page<any>>((r) => (resolveFirst = r)))
    api.getPaginated.mockImplementation(async () =>
      page([{ chemical: glucose, study: { 'grebi:nodeId': 'MTBLS9', 'grebi:name': ['Newest study'] } }])
    )
    renderTable()
    expect(screen.getByText('Loading results...')).toBeInTheDocument()

    // sort while the first page is still in flight
    fireEvent.click(screen.getAllByTestId('SwapVertIcon')[1])
    expect(await screen.findByRole('link', { name: 'Newest study' })).toBeInTheDocument()
    expect(screen.queryByText('Loading results...')).toBeNull()

    await act(async () => {
      resolveFirst(page())
    })
    expect(screen.getByRole('link', { name: 'Newest study' })).toBeInTheDocument()
    expect(screen.queryByRole('link', { name: 'Study one' })).toBeNull()
    expect(screen.queryByText('Loading results...')).toBeNull()
  })

  it('drops the facet selection when the query changes', async () => {
    const { rerender } = renderTable()
    await screen.findAllByRole('link', { name: 'glucose' })
    fireEvent.click(facetCheckbox('glucose'))
    await waitFor(() => expect(lastParams().getAll('chemical')).toEqual(['glucose']))

    rerender(
      <MemoryRouter>
        <ResultsTable graph="g" queryId="other_query" params={params} resultColumns={columns} />
      </MemoryRouter>
    )
    await waitFor(() => expect(api.getPaginated.mock.calls.at(-1)![0]).toBe('api/v1/graphs/g/query/other_query'))
    expect(lastParams().has('chemical')).toBe(false)
    expect(facetCheckbox('glucose')).not.toBeChecked()
  })
})
