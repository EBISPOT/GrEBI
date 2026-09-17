import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import SearchBox from './SearchBox'
import encodeNodeId from '../encodeNodeId'
import { Page } from '../app/api'

const api = vi.hoisted(() => ({ get: vi.fn(), getPaginated: vi.fn() }))

vi.mock('../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, get: api.get, getPaginated: api.getPaginated }
})

function LocationDisplay() {
  const loc = useLocation()
  return <div data-testid="location">{loc.pathname + loc.search}</div>
}
const location = () => new URL(screen.getByTestId('location').textContent!, 'http://x')

const nodes = [
  { 'grebi:nodeId': 'n-psoriasis', 'grebi:name': ['psoriasis'], 'grebi:type': ['biolink:Disease'], 'grebi:datasources': ['MONDO'] },
]
// the embeddings endpoint returns flat rows
const semantic = [{ nodeId: 'n-sem', name: 'psoriasis vulgaris', datasources: ['MONDO'], type: ['biolink:Disease'], sourceIds: [] }]

let models: { model: string; can_embed: boolean }[] = []

type Props = Parameters<typeof SearchBox>[0]

function renderBox(props: Partial<Props> = {}) {
  const utils = render(
    <MemoryRouter initialEntries={['/']}>
      <SearchBox graph="g" {...props} />
      <LocationDisplay />
    </MemoryRouter>
  )
  return {
    ...utils,
    input: () => screen.getByPlaceholderText('Search for knowledge about...') as HTMLInputElement,
    dropdown: () => screen.getByRole('list'),
  }
}

beforeEach(() => {
  models = []
  api.get.mockReset()
  api.getPaginated.mockReset()
  api.get.mockImplementation(async (path: string) => {
    if (path.includes('/embedding_models')) return models
    if (path.includes('/suggest?')) return ['psoriasis', 'psoriatic arthritis', 'psoriasis vulgaris']
    if (path.includes('/semantic_search?')) return semantic
    throw new Error('unexpected request ' + path)
  })
  api.getPaginated.mockImplementation(async () => new Page(0, nodes.length, 1, nodes.length, nodes, new Map()))
})

describe('SearchBox', () => {
  it('looks up the embedding models and only shows the picker when one can embed', async () => {
    const { unmount } = renderBox()
    await waitFor(() => expect(api.get).toHaveBeenCalledWith('api/v1/graphs/g/embedding_models'))
    expect(screen.queryByRole('combobox')).toBeNull()
    unmount()

    models = [{ model: 'precomputed', can_embed: false }, { model: 'minilm', can_embed: true }]
    renderBox()
    // the first embeddable model is selected by default
    expect(await screen.findByRole('combobox')).toHaveTextContent('minilm')
  })

  it('searches for the initial query straight away', async () => {
    const { input } = renderBox({ initialQuery: 'psor' })
    expect(input()).toHaveValue('psor')
    await waitFor(() => expect(api.getPaginated).toHaveBeenCalledTimes(1))
    expect(api.getPaginated.mock.calls[0][0]).toContain('q=psor&')
  })

  it('typing runs a search and a suggest, listing autocompletes (except the query itself) and nodes', async () => {
    const { input, dropdown } = renderBox()
    expect(dropdown()).toHaveClass('hidden')
    fireEvent.focus(input())
    fireEvent.change(input(), { target: { value: 'psoriasis' } })
    await waitFor(() => expect(api.getPaginated).toHaveBeenCalledTimes(1))
    expect(api.getPaginated.mock.calls[0][0]).toBe(
      'api/v1/graphs/g/search?q=psoriasis&size=5&lang=en&exactMatch=false&resolve=false'
    )
    expect(api.get).toHaveBeenCalledWith('api/v1/graphs/g/suggest?q=psoriasis')

    // node entries are re-keyed on every render, so always query afresh
    const nodeLink = () => screen.getByRole('link', { name: /psoriasis/ })
    await waitFor(() => expect(nodeLink()).toHaveAttribute('href', `/graphs/g/nodes/${encodeNodeId('n-psoriasis')}`))
    expect(nodeLink()).toHaveTextContent('psoriasisDiseaseMONDO')
    const autocompletes = screen
      .getAllByRole('listitem')
      .filter((li) => !li.querySelector('a'))
      .map((li) => li.textContent)
    expect(autocompletes).toEqual(['psoriatic arthritis', 'psoriasis vulgaris'])
    expect(dropdown()).not.toHaveClass('hidden')
    expect(screen.getByText('Search for')).toBeInTheDocument()
  })

  it('Enter searches for the typed text, or follows the highlighted entry instead', async () => {
    const { input } = renderBox()
    fireEvent.focus(input())
    fireEvent.change(input(), { target: { value: 'psoriasis' } })
    await screen.findByRole('link', { name: /psoriasis/ })

    fireEvent.keyDown(input(), { key: 'Enter' })
    expect(location().pathname).toBe('/graphs/g/search')
    expect(location().searchParams.get('q')).toBe('psoriasis')
    expect(location().searchParams.has('model')).toBe(false)

    fireEvent.keyDown(input(), { key: 'ArrowDown' })
    expect(screen.getAllByRole('listitem')[0]).toHaveClass('bg-link-light')
    fireEvent.keyDown(input(), { key: 'Enter' })
    expect(location().searchParams.get('q')).toBe('psoriatic arthritis')
  })

  it('the Search button merges additional params into the search URL', async () => {
    const { input } = renderBox({
      additionalParams: new URLSearchParams({ 'grebi:type': 'biolink:Disease' }),
      showSuggestions: false,
    })
    fireEvent.change(input(), { target: { value: 'psor' } })
    await waitFor(() => expect(api.getPaginated).toHaveBeenCalledTimes(1))
    // the filter also narrows the live search, and suggestions are off
    expect(api.getPaginated.mock.calls[0][0]).toContain('grebi%3Atype=biolink%3ADisease')
    expect(api.get).not.toHaveBeenCalledWith(expect.stringContaining('/suggest'))

    fireEvent.click(screen.getByRole('button', { name: 'Search' }))
    expect(location().pathname).toBe('/graphs/g/search')
    expect(location().searchParams.get('q')).toBe('psor')
    expect(location().searchParams.get('grebi:type')).toBe('biolink:Disease')
  })

  it('controlled with an embedding model it runs a semantic search and keeps the model in the URL', async () => {
    const { input } = renderBox({
      availableModels: [{ model: 'minilm', can_embed: true }],
      selectedModel: 'minilm',
      onModelChange: vi.fn(),
    })
    fireEvent.focus(input())
    fireEvent.change(input(), { target: { value: 'psor' } })
    await waitFor(() =>
      expect(api.get).toHaveBeenCalledWith('api/v1/graphs/g/semantic_search?q=psor&model=minilm&n=5')
    )
    expect(api.get).not.toHaveBeenCalledWith('api/v1/graphs/g/embedding_models')
    expect(api.get).not.toHaveBeenCalledWith(expect.stringContaining('/suggest'))
    expect(api.getPaginated).not.toHaveBeenCalled()

    await waitFor(() =>
      expect(screen.getByRole('link', { name: /psoriasis vulgaris/ })).toHaveAttribute(
        'href',
        `/graphs/g/nodes/${encodeNodeId('n-sem')}`
      )
    )
    fireEvent.keyDown(input(), { key: 'Enter' })
    expect(location().searchParams.get('model')).toBe('minilm')
    expect(location().searchParams.get('q')).toBe('psor')
  })

  it('picking a model reports it to the owner', async () => {
    const onModelChange = vi.fn()
    renderBox({ availableModels: [{ model: 'minilm', can_embed: true }], selectedModel: 'minilm', onModelChange })
    fireEvent.mouseDown(screen.getByRole('combobox'))
    fireEvent.click(await screen.findByRole('option', { name: 'Lexical' }))
    expect(onModelChange).toHaveBeenCalledWith('lexical')
  })
})
