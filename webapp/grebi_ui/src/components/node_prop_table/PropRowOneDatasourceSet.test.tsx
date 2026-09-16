import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import PropRowOneDatasourceSet from './PropRowOneDatasourceSet'
import { makeNode, pv, tags } from './testNode'

const node = makeNode()
const values = [pv('value-one', 'OLS.mondo', 'GWAS'), pv('value-two', 'OLS.mondo', 'GWAS')]

const renderRow = (datasources: string[]) => render(
  <MemoryRouter>
    <PropRowOneDatasourceSet graph="g" node={node} prop="grebi:name" values={values} datasources={datasources} dsEnabled={datasources} />
  </MemoryRouter>
)

describe('PropRowOneDatasourceSet', () => {
  it('shows the label, one set of datasource tags and the values', () => {
    const { container } = renderRow(['OLS.mondo', 'GWAS', 'UberGraph'])
    expect(screen.getByText('Name')).toBeInTheDocument()
    expect(tags(container)).toEqual(['mondo', 'GWAS'])
    expect(container.textContent).toContain('value-one; value-two')
  })

  it('omits the tags when the node only has one datasource', () => {
    const { container } = renderRow(['OLS.mondo'])
    expect(tags(container)).toEqual([])
    expect(container.textContent).toContain('value-one; value-two')
  })
})
