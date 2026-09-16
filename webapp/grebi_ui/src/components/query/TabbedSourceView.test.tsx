import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import TabbedSourceView from './TabbedSourceView'

const writeText = vi.fn().mockResolvedValue(undefined)
beforeEach(() => Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true }))
afterEach(() => {
  writeText.mockClear()
  delete (navigator as any).clipboard
})

const tabs = [
  { title: 'cURL', source: "curl 'http://localhost/x.csv'", lang: 'curl' },
  { title: 'Python', source: 'import requests', lang: 'python' },
  { title: 'Cypher Query', source: 'MATCH (n) RETURN n', lang: 'cypher' },
]

const pre = (container: HTMLElement) => container.querySelector('pre')!

describe('TabbedSourceView', () => {
  it('renders nothing without tabs', () => {
    const { container } = render(<TabbedSourceView tabs={[]} />)
    expect(container).toBeEmptyDOMElement()
  })

  it('shows a button per tab with the first one active and highlighted', () => {
    const { container } = render(<TabbedSourceView tabs={tabs} />)
    expect(screen.getAllByRole('button').map(b => b.textContent)).toEqual(['cURL', 'Python', 'Cypher Query', ''])
    expect(screen.getByRole('button', { name: 'cURL' })).toHaveClass('text-white')
    expect(screen.getByRole('button', { name: 'Python' })).not.toHaveClass('text-white')
    expect(pre(container).textContent).toBe(tabs[0].source)
    expect(pre(container).querySelector('.token.string')).toHaveTextContent("'http://localhost/x.csv'")
  })

  it('switches the source when another tab is chosen', () => {
    const { container } = render(<TabbedSourceView tabs={tabs} />)
    fireEvent.click(screen.getByRole('button', { name: 'Cypher Query' }))
    expect(screen.getByRole('button', { name: 'Cypher Query' })).toHaveClass('text-white')
    expect(screen.getByRole('button', { name: 'cURL' })).not.toHaveClass('text-white')
    expect(pre(container).textContent).toBe('MATCH (n) RETURN n')
    expect(pre(container).querySelector('.token.keyword')).toHaveTextContent('MATCH')
  })

  it('copies the active tab', () => {
    render(<TabbedSourceView tabs={tabs} />)
    fireEvent.click(screen.getByTitle('Copy to clipboard'))
    expect(writeText).toHaveBeenLastCalledWith(tabs[0].source)
    fireEvent.click(screen.getByRole('button', { name: 'Python' }))
    fireEvent.click(screen.getByTitle('Copy to clipboard'))
    expect(writeText).toHaveBeenLastCalledWith('import requests')
  })

  it('injects the source unchanged when there is no grammar for the language', () => {
    // NOTE: current behaviour, looks like a bug: without a Prism grammar the raw source goes into
    // dangerouslySetInnerHTML unescaped, so markup in it is rendered rather than shown.
    // ('text' would not do: Prism core ships an empty grammar for it that still escapes markup)
    const { container } = render(<TabbedSourceView tabs={[{ title: 'Plain', source: 'a <b>bold</b> word', lang: 'nonsense' }]} />)
    expect(pre(container).querySelector('b')).toHaveTextContent('bold')
    expect(pre(container).textContent).toBe('a bold word')
  })
})
