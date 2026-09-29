import { describe, it, expect } from 'vitest'
import { FTP_BASE, LATEST_RELEASE, releaseFiles, tableFileUrl } from './ftp'

describe('ftp', () => {
  it('names the files of a graph in the latest release as the release script lays them out', () => {
    expect(FTP_BASE).toBe('https://ftp.ebi.ac.uk/pub/databases/spot/kg')
    expect(LATEST_RELEASE).toBe('https://ftp.ebi.ac.uk/pub/databases/spot/kg/latest')
    const files = releaseFiles('ebi_monarch')
    expect(files.map(f => f.file)).toEqual(['ebi_monarch_neo4j.tar.xz', 'ebi_monarch_metadata.json', 'query_results/ebi_monarch/', 'postgres.tar.xz', 'release.tar.xz'])
    expect(files[2].url).toBe('https://ftp.ebi.ac.uk/pub/databases/spot/kg/latest/query_results/ebi_monarch/')
    expect(files[0].url).toBe('https://ftp.ebi.ac.uk/pub/databases/spot/kg/latest/ebi_monarch_neo4j.tar.xz')
    expect(files.every(f => f.url.startsWith(LATEST_RELEASE + '/'))).toBe(true)
  })

  it('points a materialised table at its files in the folder of its graph', () => {
    expect(tableFileUrl('ebi_monarch', 'gwas_by_disease', 'csv')).toBe('https://ftp.ebi.ac.uk/pub/databases/spot/kg/latest/query_results/ebi_monarch/gwas_by_disease.csv.gz')
    expect(tableFileUrl('dismech', 'gwas_by_disease', 'parquet')).toBe('https://ftp.ebi.ac.uk/pub/databases/spot/kg/latest/query_results/dismech/gwas_by_disease.parquet')
  })
})
