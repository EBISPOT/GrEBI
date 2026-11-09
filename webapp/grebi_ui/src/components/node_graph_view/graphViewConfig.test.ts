import { describe, it, expect } from 'vitest'
import {
  categoryOf, graphViewConfig, isHiddenByDefault, isPinned, orderedCategories, parseGraphViewConfig, PLAIN_GRAPH_VIEW_CONFIG,
} from './graphViewConfig'

describe('the shipped graph view config (configs/ui_configs/graph_view.yaml)', () => {
  it('parses', () => {
    // a config that failed to parse would have left the plain fallback in place
    expect(graphViewConfig).not.toBe(PLAIN_GRAPH_VIEW_CONFIG)
    expect(graphViewConfig.maxStems).toBe(12)
    expect(orderedCategories(graphViewConfig).at(-1)?.id).toBe('other')
  })

  // edge types of bisphenol A, asthma and exposure to nicotine on the live graph
  it.each([
    ['biolink:subclass_of', 'hierarchy'],
    ['ols:hierarchicalParent', 'hierarchy'],
    ['ols:hierarchicalAncestor', 'hierarchy'],
    ['mesh.vocab:broaderDescriptor', 'hierarchy'],
    ['biolink:broad_match', 'mappings'],
    ['mesh.vocab:preferredMappedTo', 'mappings'],
    ['oboinowl:database_cross_reference', 'mappings'],
    ['ols_top_k:matches', 'similar'],
    ['rdf:type', 'metadata'],
    ['rdfs:isDefinedBy', 'metadata'],
    ['ols:definitionProperty', 'metadata'],
    ['biolink:subsets', 'metadata'],
    ['inchi:key_string', 'metadata'],
    ['smiles:string', 'metadata'],
    ['mesh.vocab:preferredTerm', 'metadata'],
    ['ctd:exposuremarkerid', 'exposure'],
    ['primekg:exposure_disease', 'exposure'],
    ['ro:has_exposure_stimulus', 'exposure'],
    ['biolink:chemical_gene_interaction_association', 'associations'],
    ['biolink:affects', 'associations'],
    ['gwas:associated_with', 'associations'],
    ['otar:diseaseId', 'associations'],
    ['primekg:off-label use', 'associations'],
    ['biolink:in_taxon', 'taxonomy'],
    ['ro:has_role', 'chemistry'],
    ['mesh.vocab:pharmacologicalAction', 'chemistry'],
    ['ro:has_functional_parent', 'chemistry'],
    ['biolink:orthologous_to', 'genes'],
    ['biolink:interacts_with', 'interactions'],
    ['biolink:has_participant', 'interactions'],
    ['biolink:has_part', 'anatomy'],
    ['ro:mereotopologically_related_to', 'anatomy'],
    ['biolink:related_to', 'other'],
  ])('puts %s in %s', (edgeType, category) => {
    expect(categoryOf(graphViewConfig, edgeType).id).toBe(category)
  })

  it('hides similarity matches, identifiers and metadata, and all ancestors at first', () => {
    for (const t of ['ols_top_k:matches', 'rdf:type', 'inchi:string', 'ols:hierarchicalAncestor']) {
      expect(isHiddenByDefault(graphViewConfig, t)).toBe(true)
    }
    for (const t of ['biolink:subclass_of', 'ols:hierarchicalParent', 'biolink:affects', 'biolink:related_to']) {
      expect(isHiddenByDefault(graphViewConfig, t)).toBe(false)
    }
  })

  it('pins the parents from both biolink and OLS, which are not always the same', () => {
    expect(isPinned(graphViewConfig, 'biolink:subclass_of')).toBe(true)
    expect(isPinned(graphViewConfig, 'ols:hierarchicalParent')).toBe(true)
    expect(isPinned(graphViewConfig, 'biolink:affects')).toBe(false)
  })
})

describe('parseGraphViewConfig', () => {
  const config = (categories: any[], extra: object = {}) => parseGraphViewConfig({ max_stems: 5, categories, ...extra })

  it('matches whole names, with * for any run of characters and case ignored', () => {
    const c = config([
      { id: 'm', label: 'M', edge_types: ['*match'] },
      { id: 'e', label: 'E', edge_types: ['biolink:DirectionQualifierEnum#up*', 'primekg:off-label use', 'a.b:c'] },
    ])
    expect(categoryOf(c, 'biolink:broad_match').id).toBe('m')
    expect(categoryOf(c, 'SKOS:RELATEDMATCH').id).toBe('m')
    // a pattern must match the whole name
    expect(categoryOf(c, 'ols_top_k:matches').id).toBe('other')
    // characters that mean something in a regular expression are taken literally
    expect(categoryOf(c, 'biolink:DirectionQualifierEnum#upregulated').id).toBe('e')
    expect(categoryOf(c, 'primekg:off-label use').id).toBe('e')
    expect(categoryOf(c, 'a.b:c').id).toBe('e')
    expect(categoryOf(c, 'aXb:c').id).toBe('other')
  })

  it('puts an edge type in the first category that matches it', () => {
    const c = config([
      { id: 'first', label: 'First', edge_types: ['*disease*'] },
      { id: 'second', label: 'Second', edge_types: ['*exposure*'] },
    ])
    expect(categoryOf(c, 'primekg:exposure_disease').id).toBe('first')
  })

  it('hides a hidden category\'s edge types and the ones listed as hidden', () => {
    const c = config([{ id: 'meta', label: 'Meta', hidden: true, edge_types: ['rdf:type'] }], { hidden_edge_types: ['x:*'] })
    expect(isHiddenByDefault(c, 'rdf:type')).toBe(true)
    expect(isHiddenByDefault(c, 'x:anything')).toBe(true)
    expect(isHiddenByDefault(c, 'y:anything')).toBe(false)
  })

  it('takes a category named other as the one for unmatched edge types, in its place', () => {
    const c = config([
      { id: 'other', label: 'Everything else', edge_types: [] },
      { id: 'a', label: 'A', edge_types: ['a:*'] },
    ])
    expect(categoryOf(c, 'z:z').label).toBe('Everything else')
    expect(orderedCategories(c).map((x) => x.id)).toEqual(['other', 'a'])
  })

  it('without max_stems, never gathers', () => {
    expect(parseGraphViewConfig({ categories: [] }).maxStems).toBe(Infinity)
  })

  it.each([
    [[], 'the graph view config must be a mapping'],
    [{ max_stem: 3 }, 'the graph view config has unknown keys: max_stem'],
    [{ max_stems: 0 }, 'max_stems must be a number of at least 1'],
    [{ categories: {} }, 'categories must be a list'],
    [{ categories: [{ id: 'a' }] }, 'category 1 needs an id and a label'],
    [{ categories: [{ id: 'a', label: 'A', hiden: true }] }, 'category 1 has unknown keys: hiden'],
    [{ categories: [{ id: 'a', label: 'A', hidden: 'yes' }] }, 'category a: hidden must be true or false'],
    [{ categories: [{ id: 'a', label: 'A' }, { id: 'a', label: 'B' }] }, 'the category id a is used twice'],
    [{ categories: [{ id: 'a', label: 'A', edge_types: 'x:*' }] }, 'category a: edge_types must be a list of edge type patterns'],
    [{ pinned_edge_types: [''] }, 'pinned_edge_types must be a list of edge type patterns'],
  ])('rejects %j: %s', (raw, message) => {
    expect(() => parseGraphViewConfig(raw)).toThrow(message)
  })
})
