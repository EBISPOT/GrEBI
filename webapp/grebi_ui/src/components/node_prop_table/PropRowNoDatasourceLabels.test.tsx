import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import PropRowNoDatasourceLabels from './PropRowNoDatasourceLabels'
import { makeNode, pv, tags } from './testNode'

const node = makeNode()

describe('PropRowNoDatasourceLabels', () => {
  it('shows the label and values with no datasource tags', () => {
    const { container } = render(
      <MemoryRouter>
        <PropRowNoDatasourceLabels graph="g" node={node} prop="grebi:name" values={[pv('value-one', 'GWAS'), pv('value-two', 'OLS.mondo')]} />
      </MemoryRouter>
    )
    expect(screen.getByText('Name')).toBeInTheDocument()
    expect(container.textContent).toContain('value-one; value-two')
    expect(tags(container)).toEqual([])
  })

  it('resolves the label and linked values through the node refs', () => {
    render(
      <MemoryRouter>
        <PropRowNoDatasourceLabels graph="g" node={node} prop="ro:0002200" values={[pv('hp:0000001', 'OLS.mondo')]} />
      </MemoryRouter>
    )
    expect(screen.getByText('has phenotype')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'All' })).toHaveAttribute('href', '/graphs/g/nodes/aHA6MDAwMDAwMQ')
  })
})
