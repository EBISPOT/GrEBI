import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import EbiQueryOrTopicPage, { resolveTopicId } from './EbiQueryOrTopicPage'

vi.mock('./EbiQueriesHomePage', () => ({ default: ({ initialTopic }: any) => <div>queries home:{initialTopic}</div> }))
vi.mock('./EbiQueryPage', () => ({ default: () => <div>query page</div> }))
vi.mock('../../../app/api', () => ({ get: vi.fn() }))

import { get } from '../../../app/api'
const mockedGet = vi.mocked(get)

const topic = (id: string) => ({ id, name: id, type: 'Datasource', description: '', url: '' })
const topics = [topic('gwas_catalog'), topic('impc')]

function renderAt(segment: string) {
  return render(
    <MemoryRouter initialEntries={[`/graphs/g1/queries/${segment}`]}>
      <Routes>
        <Route path="/graphs/:graph/queries/:queryid" element={<EbiQueryOrTopicPage />} />
      </Routes>
    </MemoryRouter>
  )
}

describe('resolveTopicId', () => {
  const list = [topic('gwas_catalog'), topic('gwas_summary'), topic('impc'), topic('impc_phenotypes')]

  it('matches exact ids case-insensitively', () => {
    expect(resolveTopicId(list, 'IMPC')).toBe('impc')
    expect(resolveTopicId(list, 'gwas_catalog')).toBe('gwas_catalog')
  })

  it('accepts a prefix at an underscore boundary only when it is unambiguous', () => {
    expect(resolveTopicId([topic('gwas_catalog'), topic('impc')], 'gwas')).toBe('gwas_catalog')
    expect(resolveTopicId(list, 'gwas')).toBeUndefined() // gwas_catalog or gwas_summary?
    expect(resolveTopicId(list, 'gwas_cat')).toBeUndefined() // not at a boundary
    expect(resolveTopicId(list, 'nothing')).toBeUndefined()
  })
})

// The topics list is cached in module state, so the failure case runs first:
// its rejection clears the cache and the later tests populate it.
describe('EbiQueryOrTopicPage', () => {
  it('falls back to the query page when the topics cannot be loaded', async () => {
    mockedGet.mockRejectedValueOnce(new Error('down'))
    renderAt('gwas')
    expect(await screen.findByText('query page')).toBeInTheDocument()
  })

  it('shows a spinner, then the queries list for a topic segment (exact or prefix)', async () => {
    mockedGet.mockResolvedValue(topics)
    const { unmount } = renderAt('gwas')
    expect(screen.getByRole('progressbar')).toBeInTheDocument()
    expect(await screen.findByText('queries home:gwas_catalog')).toBeInTheDocument()
    unmount()

    renderAt('IMPC')
    expect(await screen.findByText('queries home:impc')).toBeInTheDocument()
  })

  it('shows the query page for anything that is not a topic, fetching the topics only once', async () => {
    const before = mockedGet.mock.calls.length
    renderAt('some_template')
    expect(await screen.findByText('query page')).toBeInTheDocument()
    expect(mockedGet.mock.calls.length).toBe(before)
    expect(mockedGet).toHaveBeenCalledWith('api/v1/topics')
  })
})
