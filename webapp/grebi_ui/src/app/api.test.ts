import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { request, get, post, put, getPaginated, Page, ApiError, describeError } from './api'

function fetchResponding(body: any, init: { ok?: boolean; status?: number; statusText?: string } = {}) {
  const fetchMock = vi.fn(async () => ({
    ok: init.ok ?? true,
    status: init.status ?? 200,
    statusText: init.statusText ?? 'OK',
    url: 'http://localhost:3000/whatever',
    json: async () => body,
  }))
  vi.stubGlobal('fetch', fetchMock)
  return fetchMock
}

const calledUrl = (fetchMock: any) => fetchMock.mock.calls[0][0] as string
const calledInit = (fetchMock: any) => fetchMock.mock.calls[0][1] as RequestInit

beforeEach(() => {
  // request() logs and times every call
  vi.spyOn(console, 'log').mockImplementation(() => {})
  vi.spyOn(console, 'time').mockImplementation(() => {})
  vi.spyOn(console, 'timeEnd').mockImplementation(() => {})
  vi.spyOn(console, 'dir').mockImplementation(() => {})
})

afterEach(() => {
  vi.unstubAllGlobals()
  vi.restoreAllMocks()
})

describe('request / get', () => {
  it('builds the URL from the API base, path and params, repeating array values', async () => {
    const fetchMock = fetchResponding({ ok: 1 })
    const res = await get('api/v1/graphs/g/search', { q: 'psoriasis', 'grebi:type': ['a', 'b'] })
    expect(res).toEqual({ ok: 1 })
    expect(calledUrl(fetchMock)).toBe('http://localhost:3000/api/v1/graphs/g/search?q=psoriasis&grebi%3Atype=a&grebi%3Atype=b')
    expect(calledInit(fetchMock)).toEqual({})
  })

  it('uses URLSearchParams verbatim and omits the query string when there are no params', async () => {
    const fetchMock = fetchResponding([])
    await get('api/v1/graphs', new URLSearchParams([['b', '2'], ['a', '1']]))
    expect(calledUrl(fetchMock)).toBe('http://localhost:3000/api/v1/graphs?b=2&a=1')

    fetchMock.mockClear()
    await get('api/v1/graphs')
    expect(calledUrl(fetchMock)).toBe('http://localhost:3000/api/v1/graphs')

    // an empty params object still appends a bare '?'
    fetchMock.mockClear()
    await get('api/v1/graphs', {})
    expect(calledUrl(fetchMock)).toBe('http://localhost:3000/api/v1/graphs?')
  })

  it('lets a caller override the API base', async () => {
    const fetchMock = fetchResponding([])
    await get('api/v1/graphs', undefined, 'https://www.ebi.ac.uk/kg/')
    expect(calledUrl(fetchMock)).toBe('https://www.ebi.ac.uk/kg/api/v1/graphs')
  })

  it("rejects with the status and the API's message when the response is not ok", async () => {
    const fetchMock = fetchResponding({ error: 'Node not found' }, { ok: false, status: 404, statusText: 'Not Found' })
    const error = await request('api/v1/graphs/g/nodes/x', undefined).catch(e => e)
    expect(error).toBeInstanceOf(ApiError)
    expect(error.status).toBe(404)
    expect(error.message).toBe('Node not found')
    expect(error.url).toBe('http://localhost:3000/whatever')
    expect(fetchMock).toHaveBeenCalledTimes(1)

    // no JSON body: the status line is the message
    vi.stubGlobal('fetch', vi.fn(async () => ({ ok: false, status: 502, statusText: 'Bad Gateway', url: 'u', json: async () => { throw new Error('not json') } })))
    const gateway = await request('api/v1/graphs', undefined).catch(e => e)
    expect(gateway.message).toBe('502 Bad Gateway')
  })

  it('describes errors for people', () => {
    expect(describeError(new ApiError(404, 'u', 'Node not found'))).toBe('Node not found')
    expect(describeError(new ApiError(500, 'u', 'boom'))).toBe('boom (HTTP 500)')
    expect(describeError(new TypeError('Failed to fetch'))).toBe('The API could not be reached')
    expect(describeError(new Error('other'))).toBe('other')
    expect(describeError('text')).toBe('text')
  })
})

describe('post / put', () => {
  it('post sends a JSON body with the right method and content type', async () => {
    const fetchMock = fetchResponding({ id: 1 })
    await post('api/v1/things', { a: 'b' }, { name: 'x' })
    expect(calledUrl(fetchMock)).toBe('http://localhost:3000/api/v1/things?a=b')
    expect(calledInit(fetchMock)).toEqual({
      method: 'POST',
      body: JSON.stringify({ name: 'x' }),
      headers: { 'content-type': 'application/json' },
    })
  })

  it('put does the same with PUT', async () => {
    const fetchMock = fetchResponding({})
    await put('api/v1/things/1', undefined, [1, 2])
    expect(calledUrl(fetchMock)).toBe('http://localhost:3000/api/v1/things/1')
    expect(calledInit(fetchMock).method).toBe('PUT')
    expect(calledInit(fetchMock).body).toBe('[1,2]')
  })
})

describe('Page', () => {
  it('maps elements while keeping the paging metadata', () => {
    const facets = new Map([['type', new Map([['Gene', 3]])]])
    const page = new Page(1, 2, 5, 9, [1, 2], facets)
    const mapped = page.map(n => ({ n }))
    expect(mapped.elements).toEqual([{ n: 1 }, { n: 2 }])
    expect(mapped.page).toBe(1)
    expect(mapped.numElements).toBe(2)
    expect(mapped.totalPages).toBe(5)
    expect(mapped.totalElements).toBe(9)
    expect(mapped.facetFieldsToCounts).toBe(facets)
  })

  it('refuses a mapper that returns nothing', () => {
    const page = new Page(0, 1, 1, 1, [1], new Map())
    expect(() => page.map(() => null)).toThrow('Page.map function returned null or undefined')
  })
})

describe('getPaginated', () => {
  it('maps a Spring-style page response', async () => {
    fetchResponding({
      number: 2, numberOfElements: 3, totalPages: 5, totalElements: 13,
      content: ['a', 'b', 'c'], facetFieldToCounts: { type: { Gene: 3 } },
    })
    const page = await getPaginated<string>('api/v1/graphs/g/search', { q: 'x' })
    expect(page).toBeInstanceOf(Page)
    expect(page.page).toBe(2)
    expect(page.numElements).toBe(3)
    expect(page.totalPages).toBe(5)
    expect(page.totalElements).toBe(13)
    expect(page.elements).toEqual(['a', 'b', 'c'])
    expect(page.facetFieldsToCounts).toEqual({ type: { Gene: 3 } })
  })

  it('falls back to "total" and to zeros for a sparse response', async () => {
    fetchResponding({ total: 7, content: ['a'] })
    let page = await getPaginated('p')
    expect(page.totalElements).toBe(7)
    expect(page.page).toBe(0)
    expect(page.totalPages).toBe(0)

    fetchResponding({})
    page = await getPaginated('p')
    expect(page.elements).toEqual([])
    expect(page.totalElements).toBe(0)
    expect(page.facetFieldsToCounts).toEqual(new Map())
  })
})
