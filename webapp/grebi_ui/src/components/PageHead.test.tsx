import { describe, it, expect, afterEach } from 'vitest'
import { render, waitFor } from '@testing-library/react'
import PageHead from './PageHead'
import { EmbeddedContext } from '../app/embedded'

afterEach(() => {
  document.title = ''
})

describe('PageHead', () => {
  it('sets the document head on the site', async () => {
    render(<PageHead><title>psoriasis</title></PageHead>)
    await waitFor(() => expect(document.title).toBe('psoriasis'))
  })

  it('leaves the document head alone inside a widget', async () => {
    document.title = 'The host page'
    render(
      <EmbeddedContext.Provider value={true}>
        <PageHead><title>psoriasis</title></PageHead>
      </EmbeddedContext.Provider>
    )
    await new Promise((r) => setTimeout(r, 50))
    expect(document.title).toBe('The host page')
  })
})
