import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import PropRow from './PropRow'
import PropVal from '../../model/PropVal'
import { makeNode, pv, tags } from './testNode'

const node = makeNode()
const datasources = node.getDatasources()

function renderRow(values: PropVal[], dsEnabled: string[], prop = 'grebi:name') {
  return render(
    <MemoryRouter>
      <PropRow graph="g" node={node} prop={prop} values={values} datasources={datasources} dsEnabled={dsEnabled} />
    </MemoryRouter>
  )
}

describe('PropRow', () => {
  it('renders nothing when no value comes from an enabled datasource', () => {
    const { container } = renderRow([pv('psoriasis', 'OLS.mondo')], ['GWAS'])
    expect(container).toBeEmptyDOMElement()
  })

  it('drops values that only disabled datasources assert', () => {
    renderRow([pv('value-one', 'OLS.mondo', 'GWAS'), pv('value-two', 'UberGraph')], ['OLS.mondo', 'GWAS'])
    expect(screen.getByText(/value-one/)).toBeInTheDocument()
    expect(screen.queryByText(/value-two/)).toBeNull()
  })

  it('omits datasource tags when only one datasource is enabled', () => {
    const { container } = renderRow([pv('value-two', 'UberGraph'), pv('value-one', 'OLS.mondo')], ['UberGraph'])
    expect(screen.getByText('Name')).toBeInTheDocument()
    expect(screen.getByText(/value-two/)).toBeInTheDocument()
    expect(tags(container)).toEqual([])
  })

  it('tags the whole row once when every value shares the same datasources', () => {
    const { container } = renderRow([pv('value-one', 'OLS.mondo', 'GWAS'), pv('value-two', 'GWAS', 'OLS.mondo')], datasources)
    expect(tags(container)).toEqual(['mondo', 'GWAS'])
    expect(container.textContent).toMatch(/Name[\s\S]*mondo[\s\S]*GWAS[\s\S]*value-one; value-two/)
  })

  it('tags each group of values separately when the datasources differ', () => {
    const { container } = renderRow([pv('value-one', 'GWAS'), pv('value-two', 'OLS.mondo')], datasources)
    expect(tags(container)).toEqual(['mondo', 'GWAS'])
    // the group with the longer datasource-set key comes first
    expect(container.textContent).toMatch(/Name[\s\S]*mondo[\s\S]*value-two[\s\S]*GWAS[\s\S]*value-one/)
  })
})
