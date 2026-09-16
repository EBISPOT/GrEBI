import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import QueryFacets from './QueryFacets'

const topics = [
  { id: 'gwas', name: 'GWAS Catalog', type: 'Datasource', description: '', url: '' },
  { id: 'chebi', name: 'ChEBI', type: 'Datasource', description: '', url: '' },
  { id: 'genetics', name: 'Genetics', type: 'Domain', description: '', url: '' },
]

type Props = Parameters<typeof QueryFacets>[0]

function renderFacets(overrides: Partial<Props> = {}) {
  const props: Props = {
    topics,
    availableInputs: ['disease_id', 'gene_id'],
    availableOutputs: ['study', 'trait'],
    selectedTopics: new Set<string>(),
    selectedInputs: new Set<string>(),
    selectedOutputs: new Set<string>(),
    onTopicsChange: vi.fn(),
    onInputsChange: vi.fn(),
    onOutputsChange: vi.fn(),
    ...overrides,
  }
  const utils = render(<QueryFacets {...props} />)
  return {
    props,
    rerender: (more: Partial<Props>) => {
      Object.assign(props, more)
      utils.rerender(<QueryFacets {...props} />)
    },
  }
}

describe('QueryFacets', () => {
  it('groups topics by type, sorted by name within each group', () => {
    renderFacets()
    expect(screen.getByText('Datasource')).toBeInTheDocument()
    expect(screen.getByText('Domain')).toBeInTheDocument()
    const names = screen.getAllByText(/^(GWAS Catalog|ChEBI|Genetics)$/).map((e) => e.textContent)
    expect(names).toEqual(['ChEBI', 'GWAS Catalog', 'Genetics'])
  })

  it('toggles a single topic in and out of the selection', () => {
    const { props, rerender } = renderFacets()
    fireEvent.click(screen.getByLabelText('ChEBI'))
    expect(props.onTopicsChange).toHaveBeenLastCalledWith(new Set(['chebi']))

    rerender({ selectedTopics: new Set(['chebi', 'genetics']) })
    expect(screen.getByLabelText('ChEBI')).toBeChecked()
    fireEvent.click(screen.getByLabelText('ChEBI'))
    expect(props.onTopicsChange).toHaveBeenLastCalledWith(new Set(['genetics']))
  })

  it('the type checkbox is indeterminate for a partial group and selects or clears the whole group', () => {
    const { props, rerender } = renderFacets({ selectedTopics: new Set(['chebi']) })
    const group = screen.getByLabelText('Datasource')
    expect(group).not.toBeChecked()
    expect(group).toHaveAttribute('data-indeterminate', 'true')

    fireEvent.click(group)
    expect(props.onTopicsChange).toHaveBeenLastCalledWith(new Set(['chebi', 'gwas']))

    rerender({ selectedTopics: new Set(['chebi', 'gwas', 'genetics']) })
    expect(screen.getByLabelText('Datasource')).toBeChecked()
    fireEvent.click(screen.getByLabelText('Datasource'))
    // the other group's selection is left alone
    expect(props.onTopicsChange).toHaveBeenLastCalledWith(new Set(['genetics']))
  })

  it('toggles inputs and outputs through their own callbacks', () => {
    const { props } = renderFacets({ selectedOutputs: new Set(['study']) })
    fireEvent.click(screen.getByLabelText('disease_id'))
    expect(props.onInputsChange).toHaveBeenLastCalledWith(new Set(['disease_id']))
    expect(props.onOutputsChange).not.toHaveBeenCalled()

    expect(screen.getByLabelText('study')).toBeChecked()
    fireEvent.click(screen.getByLabelText('study'))
    expect(props.onOutputsChange).toHaveBeenLastCalledWith(new Set())
    expect(props.onTopicsChange).not.toHaveBeenCalled()
  })

  it('offers "Clear all" only while something is selected and then empties every facet', () => {
    const { props, rerender } = renderFacets()
    expect(screen.queryByRole('button', { name: 'Clear all' })).toBeNull()

    rerender({ selectedInputs: new Set(['gene_id']) })
    fireEvent.click(screen.getByRole('button', { name: 'Clear all' }))
    expect(props.onTopicsChange).toHaveBeenCalledWith(new Set())
    expect(props.onInputsChange).toHaveBeenCalledWith(new Set())
    expect(props.onOutputsChange).toHaveBeenCalledWith(new Set())
  })
})
