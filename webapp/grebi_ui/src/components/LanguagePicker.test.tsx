import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import LanguagePicker from './LanguagePicker'

describe('LanguagePicker', () => {
  it('lists each language with its flag, mapping bare codes to a country', () => {
    render(<LanguagePicker languages={['en', 'fr', 'zh']} lang="en" onChangeLang={vi.fn()} />)
    const options = screen.getAllByRole('option')
    expect(options.map(o => o.getAttribute('value'))).toEqual(['en', 'fr', 'zh'])
    expect(options.map(o => o.textContent)).toEqual(['🇬🇧\u00a0\u00a0en', '🇫🇷\u00a0\u00a0fr', '🇨🇳\u00a0\u00a0zh'])
  })

  it('selects the current language', () => {
    render(<LanguagePicker languages={['en', 'fr']} lang="fr" onChangeLang={vi.fn()} />)
    expect((screen.getByRole('combobox') as HTMLSelectElement).value).toBe('fr')
  })

  it('reports a change of language', () => {
    const onChangeLang = vi.fn()
    render(<LanguagePicker languages={['en', 'fr']} lang="en" onChangeLang={onChangeLang} />)
    fireEvent.change(screen.getByRole('combobox'), { target: { value: 'fr' } })
    expect(onChangeLang).toHaveBeenCalledWith('fr')
  })
})
