import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import LanguagePicker, { languageName } from './LanguagePicker'

describe('LanguagePicker', () => {
  it('lists each language by its name, not a flag', () => {
    render(<LanguagePicker languages={['en', 'fr', 'zh']} lang="en" onChangeLang={vi.fn()} />)
    const options = screen.getAllByRole('option')
    expect(options.map(o => o.getAttribute('value'))).toEqual(['en', 'fr', 'zh'])
    expect(options.map(o => o.textContent)).toEqual(['English', 'French', 'Chinese'])
  })

  it('falls back to the code for a language it cannot name', () => {
    expect(languageName('fr')).toBe('French')
    expect(languageName('x-made-up')).toBe('x-made-up')
  })

  it('selects the current language', () => {
    render(<LanguagePicker languages={['en', 'fr']} lang="fr" onChangeLang={vi.fn()} />)
    expect((screen.getByRole('combobox', { name: 'Language' }) as HTMLSelectElement).value).toBe('fr')
  })

  it('reports a change of language', () => {
    const onChangeLang = vi.fn()
    render(<LanguagePicker languages={['en', 'fr']} lang="en" onChangeLang={onChangeLang} />)
    fireEvent.change(screen.getByRole('combobox'), { target: { value: 'fr' } })
    expect(onChangeLang).toHaveBeenCalledWith('fr')
  })
})
