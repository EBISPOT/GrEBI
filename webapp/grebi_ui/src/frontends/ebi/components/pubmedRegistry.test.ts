import { describe, it, expect, vi, beforeEach } from 'vitest'
import { registerPmid, resetPubmedRefs, getRegisteredRefs, subscribe, getSnapshot } from './pubmedRegistry'

beforeEach(() => resetPubmedRefs())

describe('pubmedRegistry', () => {
  it('numbers PMIDs in order of first registration and repeats the number for repeats', () => {
    expect(registerPmid('111')).toBe(1)
    expect(registerPmid('222')).toBe(2)
    expect(registerPmid('111')).toBe(1)
    expect(registerPmid('333')).toBe(3)
  })

  it('lists references by number, with cached metadata when the PMID is known', () => {
    registerPmid('999999')
    registerPmid('40323307')
    const refs = getRegisteredRefs()
    expect(refs.map((r) => [r.pmid, r.num])).toEqual([['999999', 1], ['40323307', 2]])
    expect(refs[0].entry).toBeNull()
    expect(refs[1].entry).toMatchObject({ journal: 'Bioinformatics', year: '2025' })
  })

  it('notifies subscribers on new registrations only, until they unsubscribe', () => {
    const listener = vi.fn()
    const unsubscribe = subscribe(listener)
    const before = getSnapshot()

    registerPmid('111')
    expect(listener).toHaveBeenCalledTimes(1)
    expect(getSnapshot()).toBe(before + 1)
    registerPmid('111')
    expect(listener).toHaveBeenCalledTimes(1)

    unsubscribe()
    registerPmid('222')
    expect(listener).toHaveBeenCalledTimes(1)
  })

  it('reset clears the numbering, bumps the generation and notifies', () => {
    registerPmid('111')
    const listener = vi.fn()
    subscribe(listener)
    const before = getSnapshot()

    resetPubmedRefs()
    expect(listener).toHaveBeenCalledTimes(1)
    expect(getSnapshot()).toBe(before + 1)
    expect(getRegisteredRefs()).toEqual([])
    expect(registerPmid('222')).toBe(1)
  })
})
