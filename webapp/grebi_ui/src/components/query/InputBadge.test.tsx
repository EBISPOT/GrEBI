import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import InputBadge from './InputBadge'

describe('InputBadge', () => {
  it('renders its content as code with an input icon', () => {
    render(<InputBadge>trait_id</InputBadge>)
    const badge = screen.getByText('trait_id')
    expect(badge.tagName).toBe('CODE')
    expect(badge).toHaveClass('text-sm', 'bg-blue-50')
    expect(screen.getByTestId('InputIcon')).toBeInTheDocument()
  })

  it('shrinks for the xs size', () => {
    render(<InputBadge size="xs">trait_id</InputBadge>)
    expect(screen.getByText('trait_id')).toHaveClass('text-xs')
    expect(screen.getByText('trait_id')).not.toHaveClass('text-sm')
  })
})
