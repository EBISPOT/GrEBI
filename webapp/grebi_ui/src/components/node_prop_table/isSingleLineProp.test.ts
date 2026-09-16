import { describe, it, expect } from 'vitest'
import isSingleLineProp from './isSingleLineProp'
import PropVal from '../../model/PropVal'

describe('isSingleLineProp', () => {
  it('is true up to 48 characters', () => {
    expect(isSingleLineProp(PropVal.from('psoriasis'))).toBe(true)
    expect(isSingleLineProp(PropVal.from('x'.repeat(48)))).toBe(true)
  })

  it('is false beyond 48 characters', () => {
    expect(isSingleLineProp(PropVal.from('x'.repeat(49)))).toBe(false)
  })

  it('measures non-string values by their string form', () => {
    expect(isSingleLineProp(PropVal.from(12345))).toBe(true)
    expect(isSingleLineProp(PropVal.from({ 'rdf:type': 'owl:Restriction' }))).toBe(true)
  })
})
