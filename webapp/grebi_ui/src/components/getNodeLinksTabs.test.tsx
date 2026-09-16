import { describe, it, expect, vi, beforeEach } from 'vitest'
import getNodeLinksTabs from './getNodeLinksTabs'
import GraphNode from '../model/GraphNode'
import encodeNodeId from '../encodeNodeId'
import { Page } from '../app/api'

const api = vi.hoisted(() => ({ getPaginated: vi.fn() }))

vi.mock('../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, getPaginated: api.getPaginated }
})

beforeEach(() => {
  api.getPaginated.mockReset()
  api.getPaginated.mockResolvedValue(new Page(0, 1, 42, 42, [{}], new Map()))
})

describe('getNodeLinksTabs', () => {
  it('has no extra tabs for nodes that are not genes, without touching the API', async () => {
    const disease = new GraphNode({ 'grebi:nodeId': 'n', 'grebi:type': ['biolink:Disease'] })
    expect(await getNodeLinksTabs(disease, 'g')).toEqual([])
    const untyped = new GraphNode({ 'grebi:nodeId': 'n' })
    expect(await getNodeLinksTabs(untyped, 'g')).toEqual([])
    expect(api.getPaginated).not.toHaveBeenCalled()
  })

  it('counts chemical interactions for a gene with a one-row incoming edge query', async () => {
    const gene = new GraphNode({ 'grebi:nodeId': 'n-brca2', 'grebi:type': ['biolink:Gene'] })
    const tabs = await getNodeLinksTabs(gene, 'g')
    expect(tabs).toEqual([{ tabId: 'chemical_gene_interactions', tabName: 'Chemical Interactions', count: 42 }])
    expect(api.getPaginated).toHaveBeenCalledTimes(1)
    expect(api.getPaginated).toHaveBeenCalledWith(
      `api/v1/graphs/g/nodes/${encodeNodeId('n-brca2')}/incoming_edges`,
      { size: '1', 'grebi:type': 'biolink:chemical_gene_interaction_association' }
    )
  })

  it('treats mouse genes as genes too', async () => {
    const mouseGene = new GraphNode({ 'grebi:nodeId': 'n-mgi', 'grebi:type': ['impc:MouseGene'] })
    const tabs = await getNodeLinksTabs(mouseGene, 'g')
    expect(tabs.map((t) => t.tabId)).toEqual(['chemical_gene_interactions'])
  })
})
