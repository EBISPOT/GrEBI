import { describe, it, expect } from 'vitest'
import { externalLinkForId, externalLinkForDatasource, orderSourceIds, dbIconUrl } from './dbLinks'

describe('externalLinkForId', () => {
  it('sends ontology terms to OLS4, EBI first', () => {
    const l = externalLinkForId('mondo:0005083')!
    expect(l.url).toBe('https://www.ebi.ac.uk/ols4/ontologies/mondo/classes?iri=http://purl.obolibrary.org/obo/MONDO_0005083')
    expect(l.database).toBe('Ontology Lookup Service')
    expect(l.ebi).toBe(true)
    expect(l.icon).toBe('ols.png')
  })

  it('uses the OLS-specific IRI for ontologies OBO does not host', () => {
    expect(externalLinkForId('snomedct:238564003')!.url).toBe('https://www.ebi.ac.uk/ols4/ontologies/snomed/classes?iri=http://snomed.info/id/238564003')
    expect(externalLinkForId('mesh:D011565')!.url).toContain('iri=http://id.nlm.nih.gov/mesh/D011565')
  })

  it('routes EBI resources to their own pages', () => {
    expect(externalLinkForId('chebi:17234')!.url).toBe('https://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:17234')
    expect(externalLinkForId('uniprot:P00533')!.url).toBe('https://www.uniprot.org/uniprotkb/P00533/entry')
    expect(externalLinkForId('pdb:1a1q')!.url).toBe('https://www.ebi.ac.uk/pdbe/entry/pdb/1a1q')
    expect(externalLinkForId('pubmed:40323307')!.url).toBe('https://europepmc.org/abstract/MED/40323307')
    expect(externalLinkForId('reactome:R-HSA-109582')).toMatchObject({ database: 'Reactome', icon: 'reactome.png', ebi: true })
  })

  it('falls back to Bioregistry for everything else', () => {
    expect(externalLinkForId('ncbigene:1956')).toMatchObject({ url: 'https://www.ncbi.nlm.nih.gov/gene/1956', database: 'NCBI', icon: 'ncbi.png', ebi: false })
    expect(externalLinkForId('drugbank:DB01914')!.url).toBe('https://go.drugbank.com/drugs/DB01914')
  })

  it('does not mind the case of the prefix', () => {
    expect(externalLinkForId('MONDO:0005083')!.url).toContain('MONDO_0005083')
  })

  it('refuses ids that do not fit the prefix pattern, and text that only looks like a curie', () => {
    expect(externalLinkForId('mondo:not-a-mondo-id')).toBeNull()
    expect(externalLinkForId('note: see below')).toBeNull()
    expect(externalLinkForId('to:')).toBeNull()
    expect(externalLinkForId(':123')).toBeNull()
    expect(externalLinkForId('zzzunknown:123')).toBeNull()
    expect(externalLinkForId('')).toBeNull()
    expect(externalLinkForId(undefined as any)).toBeNull()
  })

  it('handles ids that carry no prefix through the rules', () => {
    expect(externalLinkForId('GCST000187')).toMatchObject({ url: 'https://www.ebi.ac.uk/gwas/studies/GCST000187', database: 'GWAS Catalog', icon: 'gwas.png', ebi: true })
    expect(externalLinkForId('rs7903146')!.url).toBe('https://www.ebi.ac.uk/gwas/variants/rs7903146')
    expect(externalLinkForId('ENSG00000146648')!.url).toBe('https://www.ensembl.org/id/ENSG00000146648')
    expect(externalLinkForId('ENSMUSG00000017167')!.database).toBe('Ensembl')
    expect(externalLinkForId('PXD000001')!.url).toBe('https://www.ebi.ac.uk/pride/archive/projects/PXD000001')
    expect(externalLinkForId('P00533')!.database).toBe('UniProt')
    // bare MeSH numbers, hashes and InChIKeys stay plain
    expect(externalLinkForId('D011565')).toBeNull()
    expect(externalLinkForId('c98d7cf8ac9192f4ad89d69a13930b9c')).toBeNull()
    expect(externalLinkForId('WQZGKKKJIJFFOK-GASJEMHNSA-N')).toBeNull()
  })

  it('links an IRI to itself', () => {
    expect(externalLinkForId('https://omim.org/MIM/131550')).toEqual({ url: 'https://omim.org/MIM/131550', database: 'omim.org', icon: undefined, ebi: false })
    expect(externalLinkForId('https://www.ebi.ac.uk/chembl/')!.ebi).toBe(true)
  })

  it('leaves ontologies OLS4 does not serve on their own page', () => {
    expect(externalLinkForId('pr:P00533')!.url).toBe('http://purl.obolibrary.org/obo/PR_P00533')
    expect(externalLinkForId('owl:Thing')!.url).toContain('ols4/ontologies/owl/')
  })

  it("reads OBO PURL spellings as the ontology's own prefix", () => {
    expect(externalLinkForId('obo:UBERON_0002048')!.url).toBe('https://www.ebi.ac.uk/ols4/ontologies/uberon/classes?iri=http://purl.obolibrary.org/obo/UBERON_0002048')
  })

  it('escapes the characters that would break the URL', () => {
    expect(externalLinkForId('gtopdb:4536#x')!.url).toBe('https://www.guidetopharmacology.org/GRAC/LigandDisplayForward?ligandId=4536%23x')
  })
})

describe('externalLinkForDatasource', () => {
  it('knows the EBI datasources and the OLS ontologies', () => {
    expect(externalLinkForDatasource('GWAS')).toEqual({ url: 'https://www.ebi.ac.uk/gwas', database: 'GWAS Catalog', icon: 'gwas.png', ebi: true })
    expect(externalLinkForDatasource('OLS.mondo')!.url).toBe('https://www.ebi.ac.uk/ols4/ontologies/mondo')
    expect(externalLinkForDatasource('Ontologies.nonredundant')!.url).toBe('https://www.ebi.ac.uk/ols4')
    expect(externalLinkForDatasource('EFO.mappings')!.url).toBe('https://www.ebi.ac.uk/ols4/ontologies/efo')
    expect(externalLinkForDatasource('Robokop.Pharos')).toMatchObject({ url: 'https://robokop.renci.org', database: 'ROBOKOP', ebi: false })
  })

  it('returns null for datasources it does not know', () => {
    expect(externalLinkForDatasource('HelloWorld')).toBeNull()
    expect(externalLinkForDatasource('HETT_Pesticides.EU')).toBeNull()
    expect(externalLinkForDatasource('')).toBeNull()
  })
})

describe('orderSourceIds', () => {
  it('puts EBI links first, other links next and unlinked ids last, keeping the order within each group', () => {
    expect(orderSourceIds(['D011565', 'ncbigene:1956', 'mondo:0005083', 'hash', 'efo:0000676', 'omim:131550']))
      .toEqual(['mondo:0005083', 'efo:0000676', 'ncbigene:1956', 'omim:131550', 'D011565', 'hash'])
  })
})

describe('dbIconUrl', () => {
  it('lives under the public URL', () => {
    expect(dbIconUrl('ols.png')).toBe((process.env.PUBLIC_URL || '/').replace(/\/+$/, '') + '/db_icons/ols.png')
  })
})
