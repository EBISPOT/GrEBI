import { describe, it, expect } from 'vitest'
import query2code from './query2code'

const template: any = {
  id: 'gwas_studies_by_trait',
  params: [{ param_id: 'trait_id', param_name: 'Trait', param_type: 'SourceId' }],
  examples: [{ title: 'endometriosis', params: { trait_id: 'mondo:0005133' } }],
}

describe('query2code', () => {
  it('builds cURL, Python and R against the API base for the first example', () => {
    const code = query2code(template, 'test_gwas', {})
    expect(Object.keys(code)).toEqual(['cURL', 'Python', 'R'])
    expect(code.cURL.lang).toBe('bash')
    expect(code.Python.lang).toBe('python')
    expect(code.R.lang).toBe('r')
    // REACT_APP_APIURL ends in a slash that must not be doubled
    expect(code.cURL.source).toBe(
      "curl -G 'http://localhost:3000/api/v1/graphs/test_gwas/query/gwas_studies_by_trait.csv' \\\n" +
      "  --data-urlencode 'trait_id=mondo:0005133'"
    )
    expect(code.Python.source).toContain('def gwas_studies_by_trait(trait_id):')
    expect(code.Python.source).toContain('print(gwas_studies_by_trait("mondo:0005133"))')
    expect(code.R.source).toContain('gwas_studies_by_trait <- function(trait_id) {')
    expect(code.R.source).toContain('print(gwas_studies_by_trait("mondo:0005133"))')
  })

  it('handles templates with no parameters or examples', () => {
    const code = query2code({ id: 'all_studies', params: [], examples: [] } as any, 'g', {})
    expect(code.cURL.source).toBe("curl 'http://localhost:3000/api/v1/graphs/g/query/all_studies.csv'")
    expect(code.Python.source).toContain('def all_studies():')
    expect(code.R.source).toContain('response <- GET(url)')
    expect(query2code({ id: 'bare' } as any, 'g', {}).cURL.source).toContain('/query/bare.csv')
  })

  it('leaves a parameter empty when the example does not set it', () => {
    const code = query2code({ ...template, examples: [{ title: 'x', params: {} }] }, 'g', {})
    expect(code.cURL.source).toContain("--data-urlencode 'trait_id='")
    expect(code.Python.source).toContain('print(gwas_studies_by_trait(""))')
  })

  it('ignores the params argument in favour of the first example', () => {
    // NOTE: current behaviour, looks like a bug: QueryInterface passes the user's current
    // parameter values but the snippets always show the first example's values.
    const code = query2code(template, 'g', { trait_id: 'mondo:0000001' })
    expect(code.cURL.source).toContain('mondo:0005133')
    expect(code.cURL.source).not.toContain('mondo:0000001')
  })
})
