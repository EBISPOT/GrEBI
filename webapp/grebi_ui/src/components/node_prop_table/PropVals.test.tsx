import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import PropVals from './PropVals'
import PropVal from '../../model/PropVal'
import Refs from '../../model/Refs'

const refs = new Refs({
  'ro:0002200': { 'grebi:nodeId': 'ro:0002200', 'grebi:name': ['has phenotype'] },
  'hp:0000001': { 'grebi:nodeId': 'hp:0000001', 'grebi:name': ['All'] },
})

const renderVals = (values: any[], r: Refs = refs) =>
  render(<MemoryRouter><PropVals graph="g" refs={r} values={PropVal.arrFrom(values)} /></MemoryRouter>)

const short = (n: number) => Array.from({ length: n }, (_, i) => `v${i}`)
const long = (n: number) => Array.from({ length: n }, (_, i) => `long value number ${i} `.padEnd(60, '.'))

describe('PropVals', () => {
  it('refuses to render without refs', () => {
    vi.spyOn(console, 'error').mockImplementation(() => {})
    expect(() => renderVals(['x'], null as any)).toThrow('refs missing')
    vi.restoreAllMocks()
  })

  it('joins short values on one line with semicolons', () => {
    const { container } = renderVals(['a', 'b', 'c'])
    expect(container.textContent!.trim()).toBe('a; b; c')
    expect(container.querySelector('div')).toBeNull()
  })

  it('collapses more than ten short values behind a count', () => {
    const { container } = renderVals(short(12))
    expect(container.textContent).toContain('v9')
    expect(container.textContent).not.toContain('v10')
    fireEvent.click(screen.getByText('+ 2'))
    expect(container.textContent).toContain('v11')
    expect(screen.queryByText('+ 2')).toBeNull()
  })

  it('puts long values on their own lines and collapses more than five', () => {
    const { container, unmount } = renderVals(long(2))
    expect(container.firstElementChild!.children).toHaveLength(2)
    expect(container.textContent).not.toContain(';')
    unmount()

    const many = renderVals(long(7))
    expect(many.container.textContent).toContain('long value number 4')
    expect(many.container.textContent).not.toContain('long value number 5')
    fireEvent.click(screen.getByText('+ 2'))
    expect(many.container.textContent).toContain('long value number 6')
  })

  it('links values that resolve through the refs', () => {
    renderVals(['hp:0000001', 'not a ref'])
    expect(screen.getByRole('link', { name: 'All' })).toHaveAttribute('href', '/graphs/g/nodes/aHA6MDAwMDAwMQ')
    expect(screen.getByText(/not a ref/)).toBeInTheDocument()
    expect(screen.getAllByRole('link')).toHaveLength(1)
  })

  it('renders class expressions and dumps other objects as JSON', () => {
    const { container } = renderVals([
      { 'rdf:type': 'owl:Restriction', 'owl:onProperty': 'ro:0002200', 'owl:someValuesFrom': 'hp:0000001' },
      { foo: 'bar' },
    ])
    expect(container.textContent).toMatch(/has phenotype[\s\S]*some[\s\S]*All/)
    expect(screen.getByRole('link', { name: 'has phenotype' })).toBeInTheDocument()
    expect(screen.getByText('{"foo":"bar"}')).toBeInTheDocument()
  })
})
