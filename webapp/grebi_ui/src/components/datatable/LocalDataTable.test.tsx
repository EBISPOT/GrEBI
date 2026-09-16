import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, within } from '@testing-library/react'
import LocalDataTable from './LocalDataTable'
import { Column } from './DataTable'

const data = Array.from({ length: 12 }, (_, i) => ({ name: `row${i}`, kind: i % 2 ? 'odd' : 'even' }))
const columns: Column[] = [
  { id: 'name', name: 'Name', selector: (row) => row.name, sortable: true },
  { id: 'kind', name: 'Kind', selector: (row) => row.kind },
]

const rowNames = () => screen.getAllByRole('row').slice(1).map(r => within(r).getAllByRole('cell')[0].textContent)
const renderTable = (props: Partial<React.ComponentProps<typeof LocalDataTable>> = {}) =>
  render(<LocalDataTable data={data} columns={columns} defaultSelector={undefined} {...props} />)

describe('LocalDataTable', () => {
  it('shows the first ten rows by default', () => {
    renderTable()
    expect(rowNames()).toEqual(data.slice(0, 10).map(r => r.name))
  })

  it('filters rows by any value, ignoring case', () => {
    renderTable()
    fireEvent.change(screen.getByRole('textbox'), { target: { value: 'ODD' } })
    expect(rowNames()).toEqual(['row1', 'row3', 'row5', 'row7', 'row9', 'row11'])
    fireEvent.change(screen.getByRole('textbox'), { target: { value: 'row1' } })
    expect(rowNames()).toEqual(['row1', 'row10', 'row11'])
    fireEvent.change(screen.getByRole('textbox'), { target: { value: 'nothing' } })
    expect(screen.getAllByRole('row')).toHaveLength(1)
  })

  it('shows more rows when the page size grows', () => {
    renderTable()
    fireEvent.change(screen.getByRole('combobox'), { target: { value: '25' } })
    expect(rowNames()).toHaveLength(12)
  })

  it('keeps the sort state itself', () => {
    renderTable()
    fireEvent.click(screen.getByTestId('SwapVertIcon'))
    fireEvent.click(screen.getByTestId('ArrowDownwardIcon'))
    expect(screen.getByTestId('ArrowUpwardIcon')).toBeInTheDocument()
  })

  it('reports the clicked row', () => {
    const onSelectRow = vi.fn()
    renderTable({ onSelectRow })
    fireEvent.click(screen.getAllByRole('row')[3])
    expect(onSelectRow).toHaveBeenCalledWith(data[2])
  })

  it('pages with Next and Previous', () => {
    renderTable()
    const numbered = () => screen.getAllByRole('button').map(b => b.textContent).filter(t => t !== 'Previous' && t !== 'Next')
    expect(numbered()).toEqual(['1', '2'])
    expect(screen.getByRole('button', { name: 'Previous' })).toBeDisabled()

    fireEvent.click(screen.getByRole('button', { name: 'Next' }))
    expect(rowNames()).toEqual(['row10', 'row11'])
    expect(screen.getByRole('button', { name: 'Next' })).toBeDisabled()
    fireEvent.click(screen.getByRole('button', { name: 'Previous' }))
    expect(rowNames()).toHaveLength(10)
    expect(screen.getByRole('button', { name: 'Previous' })).toBeDisabled()
  })
})
