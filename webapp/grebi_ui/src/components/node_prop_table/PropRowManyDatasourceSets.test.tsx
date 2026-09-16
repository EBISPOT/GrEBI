import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import PropRowManyDatasourceSets from './PropRowManyDatasourceSets'
import PropVal from '../../model/PropVal'
import { makeNode, pv, tags } from './testNode'

const node = makeNode()
const datasources = node.getDatasources()

const renderRow = (values: PropVal[]) => render(
  <MemoryRouter>
    <PropRowManyDatasourceSets graph="g" node={node} prop="grebi:name" values={values} datasources={datasources} dsEnabled={datasources} />
  </MemoryRouter>
)

describe('PropRowManyDatasourceSets', () => {
  it('groups values by datasource set, largest set first, each with its own tags', () => {
    const { container } = renderRow([pv('value-two', 'UberGraph'), pv('value-one', 'OLS.mondo', 'GWAS')])
    expect(screen.getByText('Name')).toBeInTheDocument()
    expect(tags(container)).toEqual(['mondo', 'GWAS', 'UberGraph'])
    expect(container.textContent).toMatch(/Name[\s\S]*mondo[\s\S]*GWAS[\s\S]*value-one[\s\S]*UberGraph[\s\S]*value-two/)
  })

  it('falls back to the raw key when a group has several values or a long one', () => {
    const { container, unmount } = renderRow([pv('x', 'GWAS'), pv('y', 'GWAS'), pv('z', 'UberGraph')])
    expect(screen.getByText('grebi:name')).toBeInTheDocument()
    expect(screen.queryByText('Name')).toBeNull()
    // 'UberGraph' is the longer set key, so its group comes first
    expect(tags(container)).toEqual(['UberGraph', 'GWAS'])
    expect(container.textContent).toMatch(/UberGraph[\s\S]*z[\s\S]*GWAS[\s\S]*x; y/)
    unmount()

    renderRow([pv('a'.repeat(60), 'GWAS'), pv('short', 'UberGraph')])
    expect(screen.getByText('grebi:name')).toBeInTheDocument()
  })
})
