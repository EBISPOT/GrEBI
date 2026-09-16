import { describe, it, expect, vi, afterEach } from 'vitest'
import { renderHook } from '@testing-library/react'
import {
  asArray, randomString, sortByKeys, copyToClipboard, usePrevious, mapToApiParams, toCamel,
  pickBestDisplayName, pickWorstDisplayName, sortDisplayNamesByReadability, readabilityScore,
  difference, joinSearchParams,
} from './util'

afterEach(() => {
  vi.restoreAllMocks()
  delete (navigator as any).clipboard
  delete (document as any).execCommand
})

describe('asArray', () => {
  it('passes arrays through, wraps scalars and drops falsy values', () => {
    const arr = [1, 2]
    expect(asArray(arr)).toBe(arr)
    expect(asArray('x')).toEqual(['x'])
    expect(asArray(undefined as any)).toEqual([])
    expect(asArray(null as any)).toEqual([])
    expect(asArray(0)).toEqual([])
  })
})

describe('randomString', () => {
  it('is a base36 rendering of a random number', () => {
    expect(randomString()).toMatch(/^[0-9a-z.]+$/)
    vi.spyOn(Math, 'random').mockReturnValue(0.5)
    expect(randomString()).toBe((0.5 * Math.pow(2, 54)).toString(36))
  })
})

describe('sortByKeys', () => {
  it('orders by key, ignoring case', () => {
    const sorted = [{ key: 'b' }, { key: 'A' }, { key: 'c' }, { key: 'a' }].sort(sortByKeys)
    expect(sorted.map(x => x.key)).toEqual(['A', 'a', 'b', 'c'])
    expect(sortByKeys({ key: 'x' }, { key: 'X' })).toBe(0)
  })
})

describe('copyToClipboard', () => {
  it('uses the async clipboard API when present', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    await copyToClipboard('hello')
    expect(writeText).toHaveBeenCalledWith('hello')
  })

  it('falls back to execCommand otherwise', async () => {
    const execCommand = vi.fn().mockReturnValue(true)
    ;(document as any).execCommand = execCommand
    await expect(copyToClipboard('hello')).resolves.toBe(true)
    expect(execCommand).toHaveBeenCalledWith('copy', true, 'hello')
  })
})

describe('usePrevious', () => {
  it('returns the value from the previous render', () => {
    const { result, rerender } = renderHook(({ v }) => usePrevious(v), { initialProps: { v: 1 } })
    expect(result.current).toBeUndefined()
    rerender({ v: 2 })
    expect(result.current).toBe(1)
    rerender({ v: 3 })
    expect(result.current).toBe(2)
  })
})

describe('mapToApiParams and toCamel', () => {
  it('camel-cases snake_case keys and renames obo_id to curie', () => {
    const out = mapToApiParams(new URLSearchParams('search_term=x&q=y&obo_id=GO:1&obo_id=GO:2'))
    expect(out.get('searchTerm')).toBe('x')
    expect(out.get('q')).toBe('y')
    expect(out.getAll('curie')).toEqual(['GO:1', 'GO:2'])
    expect(out.has('obo_id')).toBe(false)
  })

  it('toCamel handles underscores and dashes', () => {
    expect(toCamel('foo_bar_baz')).toBe('fooBarBaz')
    expect(toCamel('foo-bar')).toBe('fooBar')
    expect(toCamel('plain')).toBe('plain')
  })
})

describe('display name readability', () => {
  it('scores letters and spaces up and everything else heavily down', () => {
    expect(readabilityScore('abc')).toBe(3)
    expect(readabilityScore('a b')).toBe(3)
    expect(readabilityScore('a:1')).toBe(-19)
    expect(readabilityScore('')).toBe(0)
  })

  it('picks the most and least readable names without mutating the input', () => {
    // scores: -75, 9, -64 (a space counts as readable, a colon does not)
    const names = ['mondo:0005083', 'psoriasis', 'MONDO 0005083']
    expect(pickBestDisplayName(names)).toBe('psoriasis')
    expect(pickWorstDisplayName(names)).toBe('mondo:0005083')
    expect(sortDisplayNamesByReadability(names)).toEqual(['psoriasis', 'MONDO 0005083', 'mondo:0005083'])
    expect(names).toEqual(['mondo:0005083', 'psoriasis', 'MONDO 0005083'])
    expect(pickBestDisplayName([])).toBeUndefined()
  })
})

describe('difference', () => {
  it('keeps the elements of a that are not in b', () => {
    expect(difference([1, 2, 3, 2], [2])).toEqual([1, 3])
    expect(difference([], [1])).toEqual([])
  })
})

describe('joinSearchParams', () => {
  it('copies a when there is no b', () => {
    const a = new URLSearchParams('x=1')
    const res = joinSearchParams(a)
    expect(res).not.toBe(a)
    expect(res.toString()).toBe('x=1')
  })

  it('lets b override keys from a while keeping multi-values from b', () => {
    const res = joinSearchParams(new URLSearchParams('a=1&b=2&b=22'), new URLSearchParams('b=3&b=4&c=5'))
    expect(res.toString()).toBe('a=1&b=3&b=4&c=5')
  })
})
