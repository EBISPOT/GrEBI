import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import LoadingOverlay from './LoadingOverlay'

describe('LoadingOverlay', () => {
  it('shows a spinner with the message', () => {
    const { container } = render(<LoadingOverlay message="Loading properties..." />)
    expect(screen.getByText('Loading properties...')).toBeInTheDocument()
    expect(container.querySelector('.spinner-default')).not.toBeNull()
  })

  it('covers the viewport by default and only its container when scoped', () => {
    const { container, rerender } = render(<LoadingOverlay />)
    expect(container.firstChild).toHaveClass('fixed')
    rerender(<LoadingOverlay scoped />)
    expect(container.firstChild).toHaveClass('absolute')
    expect(container.firstChild).not.toHaveClass('fixed')
  })
})
