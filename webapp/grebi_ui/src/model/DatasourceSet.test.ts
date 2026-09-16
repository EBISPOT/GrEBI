import { describe, it, expect } from 'vitest'
import DatasourceSet from './DatasourceSet'

describe('DatasourceSet', () => {
  it('dedupes the datasources into a set', () => {
    const set = new DatasourceSet(['GWAS', 'OLS.mondo', 'GWAS'])
    expect(set.datasources.size).toBe(2)
    expect(set.datasources.has('OLS.mondo')).toBe(true)
    expect(new DatasourceSet([]).datasources.size).toBe(0)
  })

  it('starts with nothing enabled', () => {
    expect(new DatasourceSet(['GWAS']).dsEnabled.size).toBe(0)
  })
})
