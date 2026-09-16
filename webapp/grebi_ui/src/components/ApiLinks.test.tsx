import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import ApiLinks from './ApiLinks'

// build.mjs bakes PUBLIC_URL into the bundle; the dev env sets it to "/"
beforeEach(() => vi.stubEnv('PUBLIC_URL', '/'))
afterEach(() => {
  vi.unstubAllEnvs()
  vi.restoreAllMocks()
})

const renderLinks = () => render(
  <MemoryRouter>
    <ApiLinks apiUrl="http://localhost:3000/api/v1/graphs/g/nodes/x" betaApiUrl="http://beta/x" />
  </MemoryRouter>
)

describe('ApiLinks', () => {
  it('links the JSON icon to the API URL in a new tab', () => {
    renderLinks()
    const link = screen.getByRole('link')
    expect(link).toHaveAttribute('href', 'http://localhost:3000/api/v1/graphs/g/nodes/x')
    expect(link).toHaveAttribute('target', '_blank')
    expect(link).toHaveAttribute('rel', 'noopener noreferrer')
    const img = screen.getByRole('img', { name: 'JSON document' })
    expect(img).toHaveAttribute('src', '/json.svg')
    // the beta link is commented out
    expect(screen.getAllByRole('link')).toHaveLength(1)
  })

  it('resolves the icon relative to PUBLIC_URL', () => {
    vi.stubEnv('PUBLIC_URL', '/kg/')
    renderLinks()
    expect(screen.getByRole('img')).toHaveAttribute('src', '/kg/json.svg')
  })

  it('falls back to the site root without a PUBLIC_URL', () => {
    const saved = process.env.PUBLIC_URL
    delete process.env.PUBLIC_URL
    try {
      renderLinks()
      expect(screen.getByRole('img')).toHaveAttribute('src', '/json.svg')
    } finally {
      process.env.PUBLIC_URL = saved
    }
  })
})
