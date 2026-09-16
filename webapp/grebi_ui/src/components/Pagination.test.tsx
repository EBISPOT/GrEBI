import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { Pagination } from './Pagination'

function renderPagination(page: number, dataCount: number, rowsPerPage = 10) {
  const onPageChange = vi.fn()
  const utils = render(<Pagination page={page} onPageChange={onPageChange} dataCount={dataCount} rowsPerPage={rowsPerPage} />)
  return { ...utils, onPageChange }
}

const numbered = () => screen.getAllByRole('button').map(b => b.textContent).filter(t => t !== 'Previous' && t !== 'Next')
const current = (container: HTMLElement) =>
  Array.from(container.querySelectorAll('.bg-neutral-default')).map(e => e.textContent)

describe('Pagination', () => {
  it('disables both arrows on a single page', () => {
    renderPagination(0, 7)
    expect(screen.getByRole('button', { name: 'Previous' })).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Next' })).toBeDisabled()
    expect(numbered()).toEqual(['1'])
  })

  it('on the first of many pages shows 1, 2, an ellipsis and the last page', () => {
    const { container, onPageChange } = renderPagination(0, 50)
    expect(numbered()).toEqual(['1', '2', '5'])
    expect(container.textContent).toContain('...')
    expect(current(container)).toEqual(['1'])
    expect(screen.getByRole('button', { name: 'Previous' })).toBeDisabled()

    fireEvent.click(screen.getByRole('button', { name: '2' }))
    expect(onPageChange).toHaveBeenLastCalledWith(1)
    fireEvent.click(screen.getByRole('button', { name: 'Next' }))
    expect(onPageChange).toHaveBeenLastCalledWith(1)
    fireEvent.click(screen.getByRole('button', { name: '5' }))
    expect(onPageChange).toHaveBeenLastCalledWith(4)
  })

  it('in the middle shows the neighbours around a non-clickable current page', () => {
    const { container, onPageChange } = renderPagination(2, 50)
    expect(numbered()).toEqual(['1', '2', '4', '5'])
    expect(current(container)).toEqual(['3'])
    expect(screen.queryByRole('button', { name: '3' })).toBeNull()
    fireEvent.click(screen.getByRole('button', { name: '2' }))
    expect(onPageChange).toHaveBeenLastCalledWith(1)
    fireEvent.click(screen.getByRole('button', { name: '4' }))
    expect(onPageChange).toHaveBeenLastCalledWith(3)
    fireEvent.click(screen.getByRole('button', { name: 'Previous' }))
    expect(onPageChange).toHaveBeenLastCalledWith(1)
  })

  it('on the last page shows the penultimate page and disables Next', () => {
    const { container, onPageChange } = renderPagination(4, 50)
    expect(numbered()).toEqual(['1', '4', '5'])
    expect(current(container)).toEqual(['5'])
    expect(screen.getByRole('button', { name: 'Next' })).toBeDisabled()
    fireEvent.click(screen.getByRole('button', { name: '4' }))
    expect(onPageChange).toHaveBeenLastCalledWith(3)
  })

  it('recomputes the page count when the data size or page size changes', () => {
    const { rerender, onPageChange } = renderPagination(0, 50)
    expect(numbered()).toEqual(['1', '2', '5'])
    rerender(<Pagination page={0} onPageChange={onPageChange} dataCount={50} rowsPerPage={25} />)
    expect(numbered()).toEqual(['1', '2'])
    rerender(<Pagination page={0} onPageChange={onPageChange} dataCount={5} rowsPerPage={25} />)
    expect(numbered()).toEqual(['1'])
  })

  it('shows a single page and disables both arrows with no rows', () => {
    renderPagination(0, 0)
    expect(numbered()).toEqual(['1'])
    expect(screen.getByRole('button', { name: 'Next' })).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Previous' })).toBeDisabled()
  })
})
