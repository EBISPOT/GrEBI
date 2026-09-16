// Shared fixtures for the node_prop_table tests: a node with three datasources,
// a couple of refs, and a PropVal factory.
import GraphNode from '../../model/GraphNode'
import PropVal from '../../model/PropVal'

export const makeNode = (extra: Record<string, any> = {}) => new GraphNode({
  'grebi:nodeId': 'mondo:0005083',
  'grebi:datasources': ['OLS.mondo', 'GWAS', 'UberGraph'],
  'grebi:name': [{ 'grebi:datasources': ['OLS.mondo', 'GWAS'], 'grebi:value': 'psoriasis' }],
  _refs: {
    'ro:0002200': { 'grebi:nodeId': 'ro:0002200', 'grebi:name': ['has phenotype'] },
    'hp:0000001': { 'grebi:nodeId': 'hp:0000001', 'grebi:name': ['All'] },
  },
  ...extra,
})

export const pv = (value: any, ...datasources: string[]) => new PropVal(datasources, {}, value)

export const tags = (container: HTMLElement) =>
  Array.from(container.querySelectorAll('.link-datasource, .link-ontology')).map(e => e.textContent)
