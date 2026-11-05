import { describe, it, expect } from 'vitest'
import { isHiddenProp } from './hiddenProps'

describe('isHiddenProp', () => {
  it('hides the structure of an entity and the OLS bookkeeping, nothing else', () => {
    for (const key of ['_refs', 'id', 'grebi:nodeId', 'grebi:edgeId', 'from', 'to', 'ols:definitionProperty', 'ols:isPreferredRoot']) {
      expect(isHiddenProp(key), key).toBe(true)
    }
    for (const key of ['grebi:name', 'grebi:type', 'ols:isObsolete', 'rdfs:isDefinedBy', 'gwas:pvalue']) {
      expect(isHiddenProp(key), key).toBe(false)
    }
  })
})
