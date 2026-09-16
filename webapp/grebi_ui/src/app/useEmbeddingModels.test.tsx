import { describe, it, expect, vi, beforeEach } from 'vitest'
import { renderHook, waitFor, act } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { useEmbeddingModels } from './useEmbeddingModels'
import { get } from './api'

vi.mock('./api', () => ({ get: vi.fn() }))
const getMock = vi.mocked(get)

const wrapperAt = (url: string) => ({ children }: { children: React.ReactNode }) =>
  <MemoryRouter initialEntries={[url]}>{children}</MemoryRouter>

const renderModels = (graph = 'g', url = '/') =>
  renderHook(({ graph }) => useEmbeddingModels(graph), { initialProps: { graph }, wrapper: wrapperAt(url) })

beforeEach(() => {
  getMock.mockReset()
})

describe('useEmbeddingModels', () => {
  it('defaults to the alphabetically first model that can embed', async () => {
    getMock.mockResolvedValue([
      { model: 'zeta', can_embed: true },
      { model: 'alpha', can_embed: false },
      { model: 'beta', can_embed: true },
    ])
    const { result } = renderModels('ebi_monarch_xspecies')
    expect(result.current.selectedModel).toBe('')
    await waitFor(() => expect(result.current.selectedModel).toBe('beta'))
    expect(result.current.availableModels).toHaveLength(3)
    expect(result.current.hasEmbeddingModels).toBe(true)
    expect(getMock).toHaveBeenCalledWith('api/v1/graphs/ebi_monarch_xspecies/embedding_models')
  })

  it('falls back to the first model overall when none can embed', async () => {
    getMock.mockResolvedValue([{ model: 'zeta', can_embed: false }, { model: 'alpha', can_embed: false }])
    const { result } = renderModels()
    await waitFor(() => expect(result.current.selectedModel).toBe('alpha'))
    expect(result.current.hasEmbeddingModels).toBe(true)
  })

  it('uses lexical search when there are no models or the request fails', async () => {
    getMock.mockResolvedValue([])
    const { result } = renderModels()
    await waitFor(() => expect(result.current.selectedModel).toBe('lexical'))
    expect(result.current.hasEmbeddingModels).toBe(false)

    getMock.mockRejectedValue(new Error('boom'))
    const failed = renderModels('other')
    await waitFor(() => expect(failed.result.current.selectedModel).toBe('lexical'))
    expect(failed.result.current.availableModels).toEqual([])
  })

  it('keeps a model chosen in the URL', async () => {
    getMock.mockResolvedValue([{ model: 'alpha', can_embed: true }])
    const { result } = renderModels('g', '/search?model=from-url')
    expect(result.current.selectedModel).toBe('from-url')
    await waitFor(() => expect(result.current.availableModels).toHaveLength(1))
    expect(result.current.selectedModel).toBe('from-url')
  })

  it('lets the caller change the model and refetches when the graph changes', async () => {
    getMock.mockResolvedValue([{ model: 'alpha', can_embed: true }])
    const { result, rerender } = renderModels('g1')
    await waitFor(() => expect(result.current.selectedModel).toBe('alpha'))
    act(() => result.current.setSelectedModel('lexical'))
    expect(result.current.selectedModel).toBe('lexical')

    rerender({ graph: 'g2' })
    await waitFor(() => expect(getMock).toHaveBeenCalledWith('api/v1/graphs/g2/embedding_models'))
    expect(getMock).toHaveBeenCalledTimes(2)
  })
})
