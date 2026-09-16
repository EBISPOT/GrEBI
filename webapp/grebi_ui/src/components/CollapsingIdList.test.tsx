import { describe, it, expect } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import CollapsingIdList from './CollapsingIdList'

const ids = (n: number) => Array.from({ length: n }, (_, i) => ({ value: `id${i}` }))

describe('CollapsingIdList', () => {
  it('shows up to three ids without an expander', () => {
    render(<CollapsingIdList ids={ids(3)} />)
    expect(screen.getByText('id0')).toBeInTheDocument()
    expect(screen.getByText('id2')).toBeInTheDocument()
    expect(screen.queryByText(/^\+/)).toBeNull()
  })

  it('collapses longer lists to three plus a count', () => {
    render(<CollapsingIdList ids={ids(5)} />)
    expect(screen.getByText('id2')).toBeInTheDocument()
    expect(screen.queryByText('id3')).toBeNull()
    expect(screen.getByText('+ 2')).toBeInTheDocument()
  })

  it('expands to the full list on click', () => {
    render(<CollapsingIdList ids={ids(5)} />)
    fireEvent.click(screen.getByText('+ 2'))
    expect(screen.getByText('id4')).toBeInTheDocument()
    expect(screen.queryByText('+ 2')).toBeNull()
  })

  it('renders nothing for an empty list', () => {
    const { container } = render(<CollapsingIdList ids={[]} />)
    expect(container.textContent).toBe('')
  })
})
