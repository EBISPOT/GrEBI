import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import ApiExample from './ApiExample'

// the parameter labels have no htmlFor, so find the input that follows the label
const paramInput = (name: string) =>
  screen.getByText(name, { selector: 'label' }).nextElementSibling as HTMLInputElement

let fetchMock: ReturnType<typeof vi.fn>

beforeEach(() => {
  fetchMock = vi.fn(async () => ({ json: async () => ({ a: 1 }) }))
  vi.stubGlobal('fetch', fetchMock)
})
afterEach(() => vi.unstubAllGlobals())

const snippet = () => document.querySelector('pre.bg-slate-900')!.textContent

describe('ApiExample', () => {
  it('shows the method, path and parameters, with code for the configured API instance', () => {
    render(<ApiExample method="GET" url="/api/v1/graphs" size="5" />)
    expect(screen.getByText('GET')).toBeInTheDocument()
    expect(screen.getByText('/api/v1/graphs')).toBeInTheDocument()
    expect(paramInput('size')).toHaveValue('5')
    for (const tab of ['cURL', 'Python', 'R']) expect(screen.getByRole('button', { name: tab })).toBeInTheDocument()
    expect(snippet()).toBe('curl "http://localhost:3000/api/v1/graphs?size=5"')

    fireEvent.click(screen.getByRole('button', { name: 'Python' }))
    expect(snippet()).toContain('requests.get(')
    expect(snippet()).toContain('"size": "5"')
  })

  it('defaults to GET and shows no parameter section when there are none', () => {
    render(<ApiExample url="/api/v1/stats" />)
    expect(screen.getByText('GET')).toBeInTheDocument()
    expect(screen.queryByText('Parameters')).toBeNull()
    expect(snippet()).toBe('curl "http://localhost:3000/api/v1/stats"')
  })

  it('updates the code when a parameter is edited', () => {
    render(<ApiExample url="/api/v1/graphs" size="5" />)
    fireEvent.change(paramInput('size'), { target: { value: '10' } })
    expect(snippet()).toBe('curl "http://localhost:3000/api/v1/graphs?size=10"')
  })

  it('runs the request against the API and pretty-prints the response', async () => {
    let resolve: (v: any) => void = () => {}
    fetchMock.mockReturnValue(new Promise((r) => { resolve = r }))
    render(<ApiExample url="/api/v1/graphs" size="5" />)

    fireEvent.click(screen.getByRole('button', { name: 'Try it' }))
    expect(screen.getByRole('button', { name: 'Running…' })).toBeDisabled()
    expect(fetchMock).toHaveBeenCalledWith('http://localhost:3000/api/v1/graphs?size=5')

    resolve({ json: async () => ({ a: 1 }) })
    expect(await screen.findByText('Response')).toBeInTheDocument()
    expect(document.querySelector('pre.whitespace-pre-wrap')).toHaveTextContent(/^\{ "a": 1 \}$/)
    expect(screen.getByRole('button', { name: 'Try it' })).toBeEnabled()
  })

  it('shows the error when the request fails', async () => {
    fetchMock.mockRejectedValue(new Error('network down'))
    render(<ApiExample url="/api/v1/graphs" />)
    fireEvent.click(screen.getByRole('button', { name: 'Try it' }))
    expect(await screen.findByText('network down')).toHaveClass('text-red-600')
  })

  it('truncates long responses', async () => {
    fetchMock.mockResolvedValue({ json: async () => ({ big: 'x'.repeat(3000) }) })
    render(<ApiExample url="/api/v1/graphs" />)
    fireEvent.click(screen.getByRole('button', { name: 'Try it' }))
    await waitFor(() => expect(screen.getByText(/\(truncated\)/)).toBeInTheDocument())
    expect(document.querySelector('pre.whitespace-pre-wrap')!.textContent!.length).toBeLessThan(2100)
  })

  it('renders its children after the example box', () => {
    render(<ApiExample url="/x"><p>explanation</p></ApiExample>)
    expect(screen.getByText('explanation')).toBeInTheDocument()
  })
})
