import { describe, it, expect } from 'vitest'
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
