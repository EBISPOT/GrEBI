import { describe, it, expect } from 'vitest'
import GraphNode from './GraphNode'
import Refs from './Refs'

const props = {
  'grebi:nodeId': 'mondo:0005083',
  'grebi:name': ['psoriasis', 'Psoriasis'],
  'grebi:synonym': [{ 'grebi:datasources': ['OLS.mondo'], 'grebi:value': 'psoriasis vulgaris' }],
  'grebi:description': ['A chronic skin disease'],
  'grebi:sourceIds': ['mondo:0005083', 'doid:8893'],
  'grebi:displayType': 'Disease',
  'grebi:type': ['biolink:Disease'],
  'ro:0002200': ['hp:0000001'],
  _refs: { 'ro:0002200': { 'grebi:nodeId': 'ro:0002200', 'grebi:name': ['has phenotype'] } },
}

describe('GraphNode', () => {
  it('exposes synonyms and descriptions as PropVals', () => {
    const node = new GraphNode(props)
    expect(node.getSynonyms().map(p => p.value)).toEqual(['psoriasis vulgaris'])
    expect(node.getSynonyms()[0].datasources).toEqual(['OLS.mondo'])
    expect(node.getDescriptions().map(p => p.value)).toEqual(['A chronic skin disease'])
    expect(node.getDescription()).toBe('A chronic skin disease')

    const bare = new GraphNode({ 'grebi:nodeId': 'x' })
    expect(bare.getSynonyms()).toEqual([])
    expect(bare.getDescriptions()).toEqual([])
    expect(bare.getDescription()).toBeUndefined()
  })

  it('links to the node page using the base64 node id', () => {
    expect(new GraphNode(props).getLinkUrl('ebi_monarch_xspecies'))
      .toBe('/graphs/ebi_monarch_xspecies/nodes/bW9uZG86MDAwNTA4Mw')
  })

  it('is bold only for a query matching a name, synonym or source id exactly', () => {
    const node = new GraphNode(props)
    expect(node.isBoldForQuery('Psoriasis')).toBe(true)
    expect(node.isBoldForQuery('psoriasis vulgaris')).toBe(true)
    expect(node.isBoldForQuery('doid:8893')).toBe(true)
    expect(node.isBoldForQuery('psoria')).toBe(false)
    expect(node.isBoldForQuery('PSORIASIS')).toBe(false)
  })

  it('is never deprecated', () => {
    expect(new GraphNode(props).isDeprecated()).toBe(false)
  })

  it('wraps _refs in a Refs and refuses when they are missing', () => {
    const refs = new GraphNode(props).getRefs()
    expect(refs).toBeInstanceOf(Refs)
    expect(refs.get('ro:0002200')?.getName()).toBe('has phenotype')
    expect(() => new GraphNode({ 'grebi:nodeId': 'x' }).getRefs()).toThrow('No refs in node')
  })

  it('lists props with name, synonym, description and displayType first and _refs left out', () => {
    const p = new GraphNode(props).getProps()
    expect(Object.keys(p)).toEqual([
      'grebi:name', 'grebi:synonym', 'grebi:description', 'grebi:displayType',
      'grebi:nodeId', 'grebi:sourceIds', 'grebi:type', 'ro:0002200',
    ])
    expect(p['grebi:name'].map(v => v.value)).toEqual(['psoriasis', 'Psoriasis'])
    expect(p['grebi:displayType'].map(v => v.value)).toEqual(['Disease'])
    expect(p['grebi:synonym'][0].datasources).toEqual(['OLS.mondo'])
  })
})
