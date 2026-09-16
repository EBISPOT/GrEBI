import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, within } from '@testing-library/react'
import DataTable, { Column } from './DataTable'

const columns: Column[] = [
  { id: 'name', name: 'Name', selector: (row) => row.name, sortable: true },
  { id: 'count', name: 'Count', selector: (row) => row.count },
]
const data = [
  { name: 'alpha', count: 1, extra: 'x' },
  { name: 'beta', count: 0, extra: 'y' },
]

const headerTexts = () => within(screen.getAllByRole('row')[0]).getAllByRole('cell').map(c => c.textContent)
const bodyRows = () => screen.getAllByRole('row').slice(1)
const cellTexts = (row: HTMLElement) => within(row).getAllByRole('cell').map(c => c.textContent)

describe('DataTable', () => {
  it('renders a header per column and each row through the column selectors', () => {
    render(<DataTable columns={columns} data={data} defaultSelector={undefined} />)
    expect(headerTexts()).toEqual(['Name', 'Count'])
    const rows = bodyRows()
    expect(rows).toHaveLength(2)
    expect(cellTexts(rows[0])).toEqual(['alpha', '1'])
    // NOTE: current behaviour, looks like a bug: any falsy cell value, including 0, is shown as "(no data)"
    expect(cellTexts(rows[1])).toEqual(['beta', '(no data)'])
  })

  it('renders only the header for no data and caps row height when asked', () => {
    const { container } = render(<DataTable columns={columns} data={[]} defaultSelector={undefined} maxRowHeight="1.5em" />)
    expect(screen.getAllByRole('row')).toHaveLength(1)
    expect(container.querySelector('tbody')).toBeEmptyDOMElement()
    const { container: c2 } = render(<DataTable columns={columns} data={data} defaultSelector={undefined} maxRowHeight="1.5em" />)
    expect(c2.querySelector('tbody td > div')).toHaveStyle({ maxHeight: '1.5em', overflowY: 'auto' })
  })

  it('reports the clicked row', () => {
    const onSelectRow = vi.fn()
    render(<DataTable columns={columns} data={data} defaultSelector={undefined} onSelectRow={onSelectRow} />)
    fireEvent.click(bodyRows()[1])
    expect(onSelectRow).toHaveBeenCalledWith(data[1])
    expect(bodyRows()[1]).toHaveClass('cursor-pointer')
  })

  it('cycles sort icons on sortable columns and reports the new sort', () => {
    const setSortColumn = vi.fn()
    const setSortDir = vi.fn()
    const sortProps = { columns, data, defaultSelector: undefined, setSortColumn, setSortDir }
    const { rerender } = render(<DataTable {...sortProps} sortDir="asc" />)

    // only the sortable column gets an icon
    expect(screen.getAllByTestId('SwapVertIcon')).toHaveLength(1)
    fireEvent.click(screen.getByTestId('SwapVertIcon'))
    expect(setSortColumn).toHaveBeenCalledWith('name')
    expect(setSortDir).toHaveBeenLastCalledWith('asc')

    rerender(<DataTable {...sortProps} sortColumn="name" sortDir="asc" />)
    expect(screen.queryByTestId('SwapVertIcon')).toBeNull()
    fireEvent.click(screen.getByTestId('ArrowDownwardIcon'))
    expect(setSortDir).toHaveBeenLastCalledWith('desc')

    rerender(<DataTable {...sortProps} sortColumn="name" sortDir="desc" />)
    fireEvent.click(screen.getByTestId('ArrowUpwardIcon'))
    expect(setSortDir).toHaveBeenLastCalledWith('asc')

    rerender(<DataTable columns={columns} data={data} defaultSelector={undefined} />)
    expect(screen.queryByTestId('SwapVertIcon')).toBeNull()
  })

  it('shows page-size and filter controls only when given their callbacks', () => {
    const onRowsPerPageChange = vi.fn()
    const onFilter = vi.fn()
    const { rerender } = render(
      <DataTable columns={columns} data={data} defaultSelector={undefined} rowsPerPage={10}
        onRowsPerPageChange={onRowsPerPageChange} onFilter={onFilter} placeholder="Find a gene" />
    )
    fireEvent.change(screen.getByRole('combobox'), { target: { value: '25' } })
    expect(onRowsPerPageChange).toHaveBeenCalledWith(25)
    fireEvent.change(screen.getByPlaceholderText('Find a gene'), { target: { value: 'brca' } })
    expect(onFilter).toHaveBeenCalledWith('brca')

    rerender(<DataTable columns={columns} data={data} defaultSelector={undefined} onFilter={onFilter} />)
    expect(screen.getByPlaceholderText('Search...')).toBeInTheDocument()
    expect(screen.queryByRole('combobox')).toBeNull()

    rerender(<DataTable columns={columns} data={data} defaultSelector={undefined} />)
    expect(screen.queryByRole('textbox')).toBeNull()
  })

  it('adds columns found in the data, skipping hidden and already-defined ones', () => {
    render(
      <DataTable columns={[columns[0]]} data={data} addColumnsFromData hideColumns={['count']}
        defaultSelector={(row, key) => row[key]} />
    )
    expect(headerTexts()).toEqual(['Name', 'extra'])
    expect(cellTexts(bodyRows()[0])).toEqual(['alpha', 'x'])
    expect(cellTexts(bodyRows()[1])).toEqual(['beta', 'y'])
  })

  it('refuses to add columns from data without a default selector', () => {
    vi.spyOn(console, 'error').mockImplementation(() => {})
    expect(() => render(<DataTable columns={columns} data={data} addColumnsFromData defaultSelector={undefined} />))
      .toThrow('need a defaultSelector')
    vi.restoreAllMocks()
  })

  it('paginates only when page, page size and a page callback are all given', () => {
    const onPageChange = vi.fn()
    const { rerender } = render(
      <DataTable columns={columns} data={data} defaultSelector={undefined} page={0} rowsPerPage={10} dataCount={30} onPageChange={onPageChange} />
    )
    fireEvent.click(screen.getByRole('button', { name: '3' }))
    expect(onPageChange).toHaveBeenCalledWith(2)

    rerender(<DataTable columns={columns} data={data} defaultSelector={undefined} rowsPerPage={10} dataCount={30} onPageChange={onPageChange} />)
    expect(screen.queryByRole('button', { name: 'Next' })).toBeNull()
  })
})
