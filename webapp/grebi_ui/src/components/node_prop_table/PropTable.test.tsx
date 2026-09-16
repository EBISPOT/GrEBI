import { describe, it, expect } from 'vitest'
import { render, screen, fireEvent, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import PropTable from './PropTable'
import { makeNode } from './testNode'

const node = makeNode({
  'grebi:datasources': ['OLS.mondo', 'UberGraph'],
  'grebi:synonym': [{ 'grebi:datasources': ['UberGraph'], 'grebi:value': 'psoriasis vulgaris' }],
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
})
