import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import OutputBadge from './OutputBadge'

describe('OutputBadge', () => {
  it('renders its content as code with an output icon', () => {
    render(<OutputBadge>study</OutputBadge>)
    const badge = screen.getByText('study')
    expect(badge.tagName).toBe('CODE')
    expect(badge).toHaveClass('text-sm', 'bg-green-50')
    expect(screen.getByTestId('OutputIcon')).toBeInTheDocument()
  })

  it('shrinks for the xs size', () => {
    render(<OutputBadge size="xs">study</OutputBadge>)
    expect(screen.getByText('study')).toHaveClass('text-xs')
    expect(screen.getByText('study')).not.toHaveClass('text-sm')
  })
})
