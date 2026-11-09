import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
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

  it('is a label and a value of the grid', () => {
    const { container } = renderRow([pv('value-one', 'OLS.mondo')], ['OLS.mondo'])
    expect(container.querySelector('dt')).toHaveTextContent('Name')
    expect(container.querySelector('dd')).toHaveTextContent('value-one')
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

  it('keeps the label when a group has several values or a long one', () => {
    const { container, unmount } = renderRow([pv('x', 'GWAS'), pv('y', 'GWAS'), pv('z', 'UberGraph')], datasources)
    expect(screen.getByText('Name')).toBeInTheDocument()
    // 'UberGraph' is the longer set key, so its group comes first
    expect(tags(container)).toEqual(['UberGraph', 'GWAS'])
    expect(container.textContent).toMatch(/UberGraph[\s\S]*z[\s\S]*GWAS[\s\S]*x; y/)
    unmount()

    renderRow([pv('a'.repeat(60), 'GWAS'), pv('short', 'UberGraph')], datasources)
    expect(screen.getByText('Name')).toBeInTheDocument()
  })

  it('resolves the label and linked values through the node refs', () => {
    renderRow([pv('hp:0000001', 'OLS.mondo')], ['OLS.mondo'], 'ro:0002200')
    expect(screen.getByText('has phenotype')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'All' })).toHaveAttribute('href', '/graphs/g/nodes/aHA6MDAwMDAwMQ')
  })

  describe('on a node loaded abridged', () => {
    // the node has 99,807 values of this property; three were loaded
    const abridged = makeNode({ 'grebi:truncated': { 'ro:0002200': 99807 } })
    const loaded = [pv('hp:0000001', 'OLS.mondo'), pv('hp:0000002', 'OLS.mondo'), pv('hp:0000003', 'GWAS')]
    const renderAbridged = (dsEnabled: string[], extra: object = {}) => render(
      <MemoryRouter>
        <PropRow graph="g" node={abridged} prop="ro:0002200" values={loaded} datasources={datasources} dsEnabled={dsEnabled} {...extra} />
      </MemoryRouter>
    )

    it('says how many of a cut property\'s values are loaded and offers the whole node', () => {
      const onLoadWhole = vi.fn()
      renderAbridged(datasources, { onLoadWhole })
      expect(screen.getByText(/The first 3 of 99,807 values are loaded\./)).toBeInTheDocument()
      fireEvent.click(screen.getByRole('button', { name: 'Load all' }))
      expect(onLoadWhole).toHaveBeenCalledTimes(1)
    })

    it('says so while the whole node loads', () => {
      renderAbridged(datasources, { onLoadWhole: vi.fn(), loadingWhole: true })
      expect(screen.getByText('Loading the rest…')).toBeInTheDocument()
      expect(screen.queryByRole('button', { name: 'Load all' })).toBeNull()
    })

    it('keeps the row when no loaded value is from an enabled datasource, as the rest may be', () => {
      const { container } = renderAbridged(['UberGraph'], { onLoadWhole: vi.fn() })
      expect(container.querySelector('dt')).toHaveTextContent('has phenotype')
      expect(screen.getByText(/The first 3 of 99,807 values are loaded\./)).toBeInTheDocument()
      expect(screen.getByRole('button', { name: 'Load all' })).toBeInTheDocument()
      expect(screen.queryByRole('link')).toBeNull()
    })

    it('says nothing on a property that is whole', () => {
      const { container } = render(
        <MemoryRouter>
          <PropRow graph="g" node={abridged} prop="grebi:name" values={[pv('psoriasis', 'OLS.mondo')]} datasources={datasources} dsEnabled={datasources} onLoadWhole={vi.fn()} />
        </MemoryRouter>
      )
      expect(container.textContent).not.toContain('are loaded')
      expect(screen.queryByRole('button', { name: 'Load all' })).toBeNull()
    })
  })
})
