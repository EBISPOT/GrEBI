import { describe, it, expect } from 'vitest'
import encodeNodeId from './encodeNodeId'

describe('encodeNodeId', () => {
  it('base64-encodes the id', () => {
    expect(encodeNodeId('abc')).toBe('YWJj')
    expect(encodeNodeId('mondo:0005083')).toBe('bW9uZG86MDAwNTA4Mw')
  })

  it('strips all trailing padding so the id is URL-safe', () => {
    expect(encodeNodeId('a')).toBe('YQ')
    expect(encodeNodeId('ab')).toBe('YWI')
    expect(encodeNodeId('gwas:rs162212')).not.toMatch(/=/)
  })

  it('round-trips through atob once padding is restored', () => {
    const enc = encodeNodeId('hp:0000001')
    expect(atob(enc + '='.repeat((4 - enc.length % 4) % 4))).toBe('hp:0000001')
  })
})
