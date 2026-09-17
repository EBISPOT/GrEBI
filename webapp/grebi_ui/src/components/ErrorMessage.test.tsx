import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import ErrorMessage from './ErrorMessage'
import { ApiError } from '../app/api'

describe('ErrorMessage', () => {
  it('says what could not be loaded and why', () => {
    render(<ErrorMessage what="The node" error={new ApiError(500, 'u', 'boom')} />)
    expect(screen.getByRole('alert')).toHaveTextContent('The node could not be loaded')
    expect(screen.getByRole('alert')).toHaveTextContent('boom (HTTP 500)')
  })

  it('says not found for a 404', () => {
    render(<ErrorMessage what="The node" error={new ApiError(404, 'u', 'Node not found')} />)
    expect(screen.getByRole('alert')).toHaveTextContent('The node not found')
    expect(screen.getByRole('alert')).toHaveTextContent('Node not found')
  })

  it('names an unreachable API', () => {
    render(<ErrorMessage what="Search results" error={new TypeError('Failed to fetch')} />)
    expect(screen.getByRole('alert')).toHaveTextContent('The API could not be reached')
  })
})
