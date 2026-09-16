import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { DatasourceTag, DatasourceTags } from './DatasourceTag'

describe('DatasourceTag', () => {
  it('shows ontology sources by ontology id with the ontology style', () => {
    render(<DatasourceTag ds="OLS.mondo" />)
    const tag = screen.getByTitle('mondo')
    expect(tag).toHaveTextContent('mondo')
    expect(tag).toHaveClass('link-ontology')
  })

  it('treats Ontologies.* like OLS.*', () => {
    render(<DatasourceTag ds="Ontologies.hp" />)
    expect(screen.getByTitle('hp')).toHaveClass('link-ontology')
  })

  it('shows other sources by full name with the datasource style', () => {
    render(<DatasourceTag ds="GWAS" />)
    const tag = screen.getByTitle('GWAS')
    expect(tag).toHaveTextContent('GWAS')
    expect(tag).toHaveClass('link-datasource')
  })
})

describe('DatasourceTags', () => {
  const many = ['GWAS', 'Monarch', 'OLS.mondo', 'Reactome', 'IMPC']

  it('shows up to three sources in full', () => {
    render(<DatasourceTags dss={many.slice(0, 3)} />)
    expect(screen.getAllByTitle(/./)).toHaveLength(3)
    expect(screen.queryByText(/^\+/)).toBeNull()
  })

  it('collapses more than three behind a count that expands on click', () => {
    render(<DatasourceTags dss={many} />)
    expect(screen.getAllByTitle(/./)).toHaveLength(3)
    expect(screen.queryByTitle('Reactome')).toBeNull()
    fireEvent.click(screen.getByText('+ 2'))
    expect(screen.getAllByTitle(/./)).toHaveLength(5)
    expect(screen.getByTitle('IMPC')).toBeInTheDocument()
    expect(screen.queryByText('+ 2')).toBeNull()
  })
})

describe('DatasourceTag links', () => {
  it('links to the datasource homepage, with its icon, when asked to', () => {
    render(<DatasourceTag ds="GWAS" linked />)
    const tag = screen.getByRole('link', { name: 'GWAS' })
    expect(tag).toHaveAttribute('href', 'https://www.ebi.ac.uk/gwas')
    expect(tag).toHaveAttribute('target', '_blank')
    expect(tag).toHaveAttribute('title', 'GWAS: GWAS Catalog')
    expect(tag).toHaveClass('link-datasource')
    expect(tag.querySelector('img')).toHaveAttribute('src', expect.stringContaining('db_icons/gwas.png'))
  })

  it('sends an ontology tag to that ontology in OLS', () => {
    render(<DatasourceTag ds="OLS.mondo" linked />)
    expect(screen.getByRole('link', { name: 'mondo' })).toHaveAttribute('href', 'https://www.ebi.ac.uk/ols4/ontologies/mondo')
  })

  it('stays a plain tag when not linked, or when the datasource is unknown', () => {
    render(<><DatasourceTag ds="GWAS" /><DatasourceTag ds="HelloWorld" linked /></>)
    expect(screen.queryByRole('link')).toBeNull()
    expect(screen.getByTitle('GWAS')).toHaveTextContent('GWAS')
    expect(screen.getByTitle('HelloWorld')).toBeInTheDocument()
  })

  it('does not let a click reach the row around it', () => {
    const onRow = vi.fn()
    render(<div onClick={onRow}><DatasourceTag ds="Reactome" linked /></div>)
    fireEvent.click(screen.getByRole('link', { name: 'Reactome' }))
    expect(onRow).not.toHaveBeenCalled()
  })

  it('passes linked through the list', () => {
    render(<DatasourceTags dss={['GWAS', 'Reactome']} linked />)
    expect(screen.getAllByRole('link')).toHaveLength(2)
  })
})
