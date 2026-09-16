import { describe, it, expect } from 'vitest'
import Refs from './Refs'
import GraphNodeRef from './GraphNodeRef'

const raw = {
  'ro:0002200': { 'grebi:nodeId': 'ro:0002200', 'grebi:name': ['has phenotype'] },
  'hp:0000001': { 'grebi:nodeId': 'hp:0000001', 'grebi:name': ['All'] },
}

describe('Refs', () => {
  it('resolves known ids to node refs and unknown ids to undefined', () => {
    const refs = new Refs(raw)
    const ref = refs.get('ro:0002200')
    expect(ref).toBeInstanceOf(GraphNodeRef)
    expect(ref?.getName()).toBe('has phenotype')
    expect(refs.get('nope')).toBeUndefined()
  })

  it('tolerates missing input and copies rather than aliases its source', () => {
    expect(new Refs(null).get('x')).toBeUndefined()
    expect(new Refs(undefined).refs).toEqual({})
    const src: any = { ...raw }
    const refs = new Refs(src)
    src['extra'] = { 'grebi:nodeId': 'extra' }
    expect(refs.get('extra')).toBeUndefined()
  })

  it('mergeWith nothing returns an equivalent copy', () => {
    const refs = new Refs(raw)
    const merged = refs.mergeWith(undefined)
    expect(merged).not.toBe(refs)
    expect(merged.get('hp:0000001')?.getName()).toBe('All')
  })

  it('mergeWith an object nests it under a "refs" key instead of merging', () => {
    // NOTE: current behaviour, looks like a bug: `{ ...this.refs, refs }` should be
    // `{ ...this.refs, ...refs }`, so merged-in ids are not resolvable.
    const merged = new Refs(raw).mergeWith({ 'mondo:0005083': { 'grebi:nodeId': 'mondo:0005083' } })
    expect(merged.get('mondo:0005083')).toBeUndefined()
    expect(merged.get('ro:0002200')?.getName()).toBe('has phenotype')
    expect(Object.keys(merged.refs)).toContain('refs')
  })
})
