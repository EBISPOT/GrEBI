import { describe, it, expect, vi, beforeEach } from 'vitest'
import { renderHook, waitFor } from '@testing-library/react'
import useEffectiveGraph from './useEffectiveGraph'

vi.mock('../../app/api', () => ({ get: vi.fn() }))
import { get } from '../../app/api'
const mockedGet = vi.mocked(get)

const KEY = 'grebi_last_graph'

beforeEach(() => {
  sessionStorage.clear()
  mockedGet.mockReset()
  mockedGet.mockResolvedValue(['first_graph', 'second_graph'])
})

describe('useEffectiveGraph', () => {
  it('returns the given graph and remembers it for later', () => {
    const { result } = renderHook(() => useEffectiveGraph('g1'))
    expect(result.current).toBe('g1')
    expect(sessionStorage.getItem(KEY)).toBe('g1')
    expect(mockedGet).not.toHaveBeenCalled()
  })

  it('treats blank, "undefined" and "null" as no graph and uses the remembered one instead', () => {
    sessionStorage.setItem(KEY, 'remembered')
    for (const junk of ['', '  ', 'undefined', 'null', undefined]) {
      const { result, unmount } = renderHook(() => useEffectiveGraph(junk))
      expect(result.current).toBe('remembered')
      unmount()
    }
    expect(mockedGet).not.toHaveBeenCalled()
  })

  it('asks the API for the graph list when nothing is known and remembers the first one', async () => {
    const { result } = renderHook(() => useEffectiveGraph(undefined))
    expect(result.current).toBeUndefined()
    await waitFor(() => expect(result.current).toBe('first_graph'))
    expect(mockedGet).toHaveBeenCalledWith('api/v1/graphs')
    expect(sessionStorage.getItem(KEY)).toBe('first_graph')
  })

  it('follows changes to the graph argument', () => {
    const { result, rerender } = renderHook((graph: string | undefined) => useEffectiveGraph(graph), { initialProps: 'g1' })
    rerender('g2')
    expect(result.current).toBe('g2')
    expect(sessionStorage.getItem(KEY)).toBe('g2')
    // dropping the graph keeps the last one
    rerender(undefined)
    expect(result.current).toBe('g2')
  })

  it('ignores the API answer if the hook was unmounted meanwhile', async () => {
    let resolve: (v: string[]) => void = () => {}
    mockedGet.mockReturnValue(new Promise((r) => { resolve = r }))
    const { unmount } = renderHook(() => useEffectiveGraph(undefined))
    unmount()
    resolve(['late_graph'])
    await new Promise((r) => setTimeout(r, 0))
    expect(sessionStorage.getItem(KEY)).toBeNull()
  })

  it('stays undefined when the API fails or returns no graphs', async () => {
    mockedGet.mockRejectedValueOnce(new Error('down'))
    const failed = renderHook(() => useEffectiveGraph(undefined))
    await new Promise((r) => setTimeout(r, 0))
    expect(failed.result.current).toBeUndefined()
    failed.unmount()

    mockedGet.mockResolvedValueOnce([])
    const empty = renderHook(() => useEffectiveGraph(undefined))
    await new Promise((r) => setTimeout(r, 0))
    expect(empty.result.current).toBeUndefined()
    expect(sessionStorage.getItem(KEY)).toBeNull()
  })
})
