import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, within } from '@testing-library/react'
import DatasourceSelector from './DatasourceSelector'

const datasources = ['OLS.mondo', 'GWAS', 'Ontologies.hp', 'Monarch']

function renderSelector(dsEnabled: string[], extra: Partial<React.ComponentProps<typeof DatasourceSelector>> = {}) {
  const setDsEnabled = vi.fn()
  const utils = render(<DatasourceSelector datasources={datasources} dsEnabled={dsEnabled} setDsEnabled={setDsEnabled} {...extra} />)
  return { ...utils, setDsEnabled }
}

const checkboxFor = (title: string) => within(screen.getByTitle(title).parentElement!).getByRole('checkbox')

describe('DatasourceSelector', () => {
  it('sorts sources alphabetically with ontologies last, labelled by ontology id', () => {
    const { container } = renderSelector([])
    const titles = Array.from(container.querySelectorAll('span[title]')).map(s => s.getAttribute('title'))
    expect(titles).toEqual(['GWAS', 'Monarch', 'mondo', 'hp'])
    expect(screen.getByTitle('mondo')).toHaveClass('link-ontology')
    expect(screen.getByTitle('GWAS')).toHaveClass('link-datasource')
  })

  it('reflects which sources are enabled and toggles them', () => {
    const { setDsEnabled } = renderSelector(['GWAS', 'OLS.mondo'])
    expect(checkboxFor('GWAS')).toBeChecked()
    expect(checkboxFor('Monarch')).not.toBeChecked()

    fireEvent.click(checkboxFor('GWAS'))
    expect(setDsEnabled).toHaveBeenLastCalledWith(['OLS.mondo'])

    fireEvent.click(checkboxFor('Monarch'))
    expect(setDsEnabled).toHaveBeenLastCalledWith(['GWAS', 'OLS.mondo', 'Monarch'])
  })

  it('offers All and None, each disabled when already in effect', () => {
    const { setDsEnabled, rerender } = renderSelector([])
    expect(screen.getByRole('button', { name: 'None' })).toBeDisabled()
    fireEvent.click(screen.getByRole('button', { name: 'All' }))
    expect(setDsEnabled).toHaveBeenCalledWith(datasources)

    rerender(<DatasourceSelector datasources={datasources} dsEnabled={[...datasources]} setDsEnabled={setDsEnabled} />)
    expect(screen.getByRole('button', { name: 'All' })).toBeDisabled()
    fireEvent.click(screen.getByRole('button', { name: 'None' }))
    expect(setDsEnabled).toHaveBeenLastCalledWith([])
  })

  it('shows a single source as a plain tag with no controls', () => {
    render(<DatasourceSelector datasources={['GWAS']} dsEnabled={['GWAS']} setDsEnabled={vi.fn()} />)
    expect(screen.getByTitle('GWAS')).toBeInTheDocument()
    expect(screen.queryByRole('checkbox')).toBeNull()
    expect(screen.queryByRole('button')).toBeNull()
  })

  it('reports hover in and out of a source', () => {
    const onMouseoverDs = vi.fn()
    const onMouseoutDs = vi.fn()
    renderSelector([], { onMouseoverDs, onMouseoutDs })
    fireEvent.mouseEnter(screen.getByTitle('mondo'))
    expect(onMouseoverDs).toHaveBeenCalledWith('OLS.mondo')
    fireEvent.mouseLeave(screen.getByTitle('mondo'))
    expect(onMouseoutDs).toHaveBeenCalledWith('OLS.mondo')
  })
})
