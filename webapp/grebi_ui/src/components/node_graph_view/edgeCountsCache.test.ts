import { describe, it, expect, vi, beforeEach } from 'vitest'

vi.mock('../../app/api', () => ({ get: vi.fn() }))

type CacheModule = typeof import('./edgeCountsCache')

// The cache is module state, so each test gets a fresh copy of the module
// (and of the mocked api it imports).
let cache: CacheModule
let get: ReturnType<typeof vi.fn>

beforeEach(async () => {
  vi.resetModules()
  const api: any = await import('../../app/api')
  get = api.get
  get.mockReset()
  cache = await import('./edgeCountsCache')
})

const flush = () => new Promise((r) => setTimeout(r, 0))

describe('fetchNodeEdgeCounts', () => {
  it('requests the edge_counts endpoint and fills in a missing direction', async () => {
    get.mockResolvedValue({ incoming: { is_a: { ols: 2 } } })
    const res = await cache.fetchNodeEdgeCounts('g', 'ENC')
    expect(get).toHaveBeenCalledWith('api/v1/graphs/g/nodes/ENC/edge_counts')
    expect(res).toEqual({ incoming: { is_a: { ols: 2 } }, outgoing: {} })
  })

  it('caches per graph and node', async () => {
    get.mockResolvedValue({ incoming: {}, outgoing: {} })
    await cache.fetchNodeEdgeCounts('g', 'n')
    await cache.fetchNodeEdgeCounts('g', 'n')
    expect(get).toHaveBeenCalledTimes(1)
    await cache.fetchNodeEdgeCounts('other', 'n')
    expect(get).toHaveBeenCalledTimes(2)
  })

  it('shares one in-flight request between concurrent callers', async () => {
    let resolve: (v: any) => void = () => {}
    get.mockReturnValue(new Promise((r) => { resolve = r }))
    const p1 = cache.fetchNodeEdgeCounts('g', 'n')
    const p2 = cache.fetchNodeEdgeCounts('g', 'n')
    expect(get).toHaveBeenCalledTimes(1)
    resolve({ incoming: {}, outgoing: { x: { d: 1 } } })
    const [r1, r2] = await Promise.all([p1, p2])
    expect(r1).toBe(r2)
    expect(r1.outgoing).toEqual({ x: { d: 1 } })
    // once settled, further calls are served from the cache
    await cache.fetchNodeEdgeCounts('g', 'n')
    expect(get).toHaveBeenCalledTimes(1)
  })

  it('does not cache failures, so the next call retries', async () => {
    get.mockRejectedValueOnce(new Error('down')).mockResolvedValueOnce({ incoming: {}, outgoing: {} })
    await expect(cache.fetchNodeEdgeCounts('g', 'n')).rejects.toThrow('down')
    await expect(cache.fetchNodeEdgeCounts('g', 'n')).resolves.toEqual({ incoming: {}, outgoing: {} })
    expect(get).toHaveBeenCalledTimes(2)
  })

  it('prefetch swallows errors and warms the cache on success', async () => {
    get.mockRejectedValueOnce(new Error('down'))
    cache.prefetchNodeEdgeCounts('g', 'n')
    await flush()
    expect(get).toHaveBeenCalledTimes(1)

    get.mockResolvedValueOnce({ incoming: { a: { d: 1 } }, outgoing: {} })
    cache.prefetchNodeEdgeCounts('g', 'n')
    await flush()
    await expect(cache.fetchNodeEdgeCounts('g', 'n')).resolves.toEqual({ incoming: { a: { d: 1 } }, outgoing: {} })
    expect(get).toHaveBeenCalledTimes(2)
  })

  it('evicts the oldest entry once 500 nodes are cached', async () => {
    get.mockImplementation(async () => ({ incoming: {}, outgoing: {} }))
    for (let i = 0; i < 500; i++) {
      await cache.fetchNodeEdgeCounts('g', 'n' + i)
    }
    expect(get).toHaveBeenCalledTimes(500)
    await cache.fetchNodeEdgeCounts('g', 'n0')
    expect(get).toHaveBeenCalledTimes(500)

    await cache.fetchNodeEdgeCounts('g', 'n500') // 501st entry evicts n0
    await cache.fetchNodeEdgeCounts('g', 'n0') // refetched, and in turn evicts n1
    expect(get).toHaveBeenCalledTimes(502)
    await cache.fetchNodeEdgeCounts('g', 'n2') // n2 is still cached
    expect(get).toHaveBeenCalledTimes(502)
    await cache.fetchNodeEdgeCounts('g', 'n1')
    expect(get).toHaveBeenCalledTimes(503)
  })
})
