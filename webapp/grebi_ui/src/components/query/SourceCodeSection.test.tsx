import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import SourceCodeSection from './SourceCodeSection'

const writeText = vi.fn().mockResolvedValue(undefined)
beforeEach(() => Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true }))
afterEach(() => {
  writeText.mockClear()
  delete (navigator as any).clipboard
})

const source = 'MATCH (n:Gene) RETURN n'
const renderSection = () => render(<SourceCodeSection title="Cypher" source={source} lang="Cypher" />)

describe('SourceCodeSection', () => {
  it('starts as a closed button', () => {
    renderSection()
    expect(screen.getByRole('button', { name: 'Cypher' })).toBeInTheDocument()
    expect(screen.queryByRole('dialog')).toBeNull()
  })

  it('opens a dialog with the syntax-highlighted source', async () => {
    renderSection()
    fireEvent.click(screen.getByRole('button', { name: 'Cypher' }))
    const dialog = await screen.findByRole('dialog')
    const pre = dialog.querySelector('pre')!
    expect(pre.textContent).toBe(source)
    expect(Array.from(pre.querySelectorAll('.token.keyword')).map(e => e.textContent)).toEqual(['MATCH', 'RETURN'])
  })

  it('copies the raw source to the clipboard', async () => {
    renderSection()
    fireEvent.click(screen.getByRole('button', { name: 'Cypher' }))
    fireEvent.click(await screen.findByTitle('Copy to clipboard'))
    expect(writeText).toHaveBeenCalledWith(source)
  })

  it('closes again from the close button', async () => {
    renderSection()
    fireEvent.click(screen.getByRole('button', { name: 'Cypher' }))
    await screen.findByRole('dialog')
    fireEvent.click(screen.getByTestId('CloseIcon').closest('button')!)
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
  })
})
