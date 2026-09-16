import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import SourceIdChip from './SourceIdChip'

const util = vi.hoisted(() => ({ copyToClipboard: vi.fn() }))
vi.mock('../app/util', async (importOriginal) => ({ ...(await importOriginal<any>()), copyToClipboard: util.copyToClipboard }))

describe('SourceIdChip', () => {
  it('links a known id to its database, with the icon, and keeps the copy button', () => {
    render(<SourceIdChip id="mondo:0005083" />)
    const link = screen.getByRole('link', { name: 'mondo:0005083' })
    expect(link).toHaveAttribute('href', expect.stringContaining('ols4/ontologies/mondo'))
    expect(link).toHaveAttribute('target', '_blank')
    expect(link).toHaveAttribute('title', 'Open in Ontology Lookup Service')
    expect(link.querySelector('img')).toHaveAttribute('src', expect.stringContaining('db_icons/ols.png'))
    expect(screen.getByRole('button', { name: 'Copy' })).toBeInTheDocument()
  })

  it('shows an unknown id as plain text, still with the copy button', () => {
    render(<SourceIdChip id="c98d7cf8ac9192f4ad89d69a13930b9c" />)
    expect(screen.queryByRole('link')).toBeNull()
    expect(screen.getByText('c98d7cf8ac9192f4ad89d69a13930b9c')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Copy' }))
    expect(util.copyToClipboard).toHaveBeenCalledWith('c98d7cf8ac9192f4ad89d69a13930b9c')
  })

  it('uses a generic glyph when the database has no icon', () => {
    render(<SourceIdChip id="omim:131550" />)
    const link = screen.getByRole('link', { name: 'omim:131550' })
    expect(link).toHaveAttribute('href', 'https://omim.org/entry/131550')
    expect(link.querySelector('img')).toBeNull()
    expect(link.querySelector('svg')).not.toBeNull()
  })
})
