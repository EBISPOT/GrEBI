import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter, Routes, Route, Link } from 'react-router-dom'
import ErrorBoundary, { RouteErrorBoundary } from './ErrorBoundary'

function Broken(): JSX.Element {
  throw new Error('render exploded')
}

afterEach(() => vi.restoreAllMocks())

describe('ErrorBoundary', () => {
  it('shows the error in place of the page with a way home', () => {
    vi.spyOn(console, 'error').mockImplementation(() => {})
    render(<MemoryRouter><ErrorBoundary><Broken /></ErrorBoundary></MemoryRouter>)
    expect(screen.getByRole('alert')).toHaveTextContent('This page could not be loaded')
    expect(screen.getByRole('alert')).toHaveTextContent('render exploded')
    expect(screen.getByRole('link', { name: 'Return to home' })).toHaveAttribute('href', '/')
  })

  it('clears the error on the next navigation', () => {
    vi.spyOn(console, 'error').mockImplementation(() => {})
    render(
      <MemoryRouter initialEntries={['/broken']}>
        <RouteErrorBoundary>
          <Routes>
            <Route path="/broken" element={<Broken />} />
            <Route path="/fine" element={<div>all fine</div>} />
          </Routes>
        </RouteErrorBoundary>
        <Link to="/fine">go</Link>
      </MemoryRouter>
    )
    expect(screen.getByRole('alert')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('link', { name: 'go' }))
    expect(screen.queryByRole('alert')).toBeNull()
    expect(screen.getByText('all fine')).toBeInTheDocument()
  })
})
