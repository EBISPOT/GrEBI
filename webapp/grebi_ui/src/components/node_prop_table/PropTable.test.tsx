import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import PropTable from './PropTable'
import { makeNode } from './testNode'

const node = makeNode({
  'grebi:datasources': ['OLS.mondo', 'UberGraph'],
  'grebi:synonym': [{ 'grebi:datasources': ['UberGraph'], 'grebi:value': 'psoriasis vulgaris' }],
  'ols:definitionProperty': [{ 'grebi:datasources': ['OLS.mondo'], 'grebi:value': 'iao:0000115' }],
  'ols:isPreferredRoot': [{ 'grebi:datasources': ['OLS.mondo'], 'grebi:value': 'false' }],
  id: 'should-not-show',
})

const checkboxFor = (title: string) => within(screen.getByTitle(title).parentElement!).getByRole('checkbox')
const renderTable = () => render(<MemoryRouter><PropTable graph="g" node={node} lang="en" /></MemoryRouter>)

describe('PropTable', () => {
  it('enables every datasource except UberGraph by default and shows only their values', () => {
    const { container } = renderTable()
    expect(checkboxFor('mondo')).toBeChecked()
    expect(checkboxFor('UberGraph')).not.toBeChecked()
    expect(screen.getByText('Name')).toBeInTheDocument()
    expect(container.textContent).toContain('psoriasis')
    expect(container.textContent).not.toContain('psoriasis vulgaris')
    expect(container.textContent).not.toContain('should-not-show')
    expect(screen.queryByText('Synonym')).toBeNull()
    // the OLS bookkeeping is never shown
    expect(container.textContent).not.toContain('iao:0000115')
    expect(screen.queryByText('ols:isPreferredRoot')).toBeNull()
  })

  it('reveals the values of a datasource once it is enabled, tagging rows by source', () => {
    const { container } = renderTable()
    fireEvent.click(checkboxFor('UberGraph'))
    expect(screen.getByText('Synonym')).toBeInTheDocument()
    expect(container.textContent).toContain('psoriasis vulgaris')
    // the selector tag plus a (linked) tag on the synonym row
    expect(screen.getAllByTitle(/^UberGraph/)).toHaveLength(2)
  })

  it('hides the values of a datasource once it is disabled', () => {
    const { container } = renderTable()
    fireEvent.click(checkboxFor('mondo'))
    expect(screen.queryByText('Name')).toBeNull()
    expect(container.textContent).not.toContain('psoriasis')
  })

  it('offers the whole node on the properties a node loaded abridged had cut short', () => {
    const onLoadWhole = vi.fn()
    const abridged = makeNode({
      'grebi:datasources': ['OLS.mondo'],
      'ro:0002200': [{ 'grebi:datasources': ['OLS.mondo'], 'grebi:value': 'hp:0000001' }],
      'grebi:truncated': { 'ro:0002200': 7567 },
      'embedding:m': [0.1, 0.2],
    })
    const { container, rerender } = render(<MemoryRouter><PropTable graph="g" node={abridged} lang="en" onLoadWhole={onLoadWhole} /></MemoryRouter>)
    expect(screen.getByText(/The first 1 of 7,567 values are loaded\./)).toBeInTheDocument()
    // neither the count of what was cut nor a vector is a row
    expect(container.textContent).not.toContain('grebi:truncated')
    expect(container.textContent).not.toContain('embedding:m')
    fireEvent.click(screen.getByRole('button', { name: 'Load all' }))
    expect(onLoadWhole).toHaveBeenCalledTimes(1)

    rerender(<MemoryRouter><PropTable graph="g" node={abridged} lang="en" onLoadWhole={onLoadWhole} loadingWhole /></MemoryRouter>)
    expect(screen.getByText('Loading the rest…')).toBeInTheDocument()
  })
})
