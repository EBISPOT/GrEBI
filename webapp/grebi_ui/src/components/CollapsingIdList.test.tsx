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

  it('links ids of a known kind to their database, EBI first, and leaves the rest plain', () => {
    render(<CollapsingIdList ids={[{ value: 'D011565' }, { value: 'ncbigene:1956' }, { value: 'mondo:0005083' }]} />)
    const links = screen.getAllByRole('link')
    expect(links.map((l) => l.textContent)).toEqual(['mondo:0005083', 'ncbigene:1956'])
    expect(links[0]).toHaveAttribute('href', expect.stringContaining('ols4/ontologies/mondo'))
    expect(links[0].querySelector('img')).toHaveAttribute('src', expect.stringContaining('db_icons/ols.png'))
    expect(screen.getByText('D011565').tagName).toBe('SPAN')
  })
})
