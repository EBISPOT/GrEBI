import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import QueryTemplateExample from './QueryTemplateExample'

// the parameter labels have no htmlFor, so find the input that follows the label
const paramInput = (name: string) =>
  screen.getByText(name, { selector: 'label' }).nextElementSibling as HTMLInputElement

let fetchMock: ReturnType<typeof vi.fn>
let graphs: string[] = ['g1']

beforeEach(() => {
  graphs = ['g1']
  fetchMock = vi.fn(async (url: string) => {
    if (url === 'http://localhost:3000/api/v1/graphs') return { json: async () => graphs }
    return { json: async () => ({ rows: [{ study: 'GCST1' }] }) }
  })
  vi.stubGlobal('fetch', fetchMock)
})
afterEach(() => vi.unstubAllGlobals())

const snippet = () => document.querySelector('pre.bg-slate-900')!.textContent

describe('QueryTemplateExample', () => {
  it('shows the query id, graph and parameters, and checks the graph is loaded on this instance', async () => {
    render(<QueryTemplateExample id="gwas_by_trait" graph="g1" trait_id="mondo:1" />)
    expect(screen.getByText('Query')).toBeInTheDocument()
    expect(screen.getByText('gwas_by_trait')).toBeInTheDocument()
    expect(screen.getByText('graph: g1')).toBeInTheDocument()
    expect(paramInput('trait_id')).toHaveValue('mondo:1')

    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith('http://localhost:3000/api/v1/graphs'))
    await waitFor(() => expect(screen.getByRole('button', { name: 'Try it' })).toBeEnabled())
    expect(screen.queryByText('Graph not loaded')).toBeNull()
  })

  it('warns and disables running when the graph is not loaded, or the check fails', async () => {
    graphs = ['other']
    const { unmount } = render(<QueryTemplateExample id="q" graph="g1" />)
    expect(await screen.findByText('Graph not loaded')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Try it' })).toBeDisabled()
    unmount()

    fetchMock.mockRejectedValue(new Error('down'))
    render(<QueryTemplateExample id="q" graph="g1" />)
    expect(await screen.findByText('Graph not loaded')).toBeInTheDocument()
  })

  it('generates code that reads the CSV endpoint with the parameters', () => {
    render(<QueryTemplateExample id="gwas_by_trait" graph="g1" trait_id="mondo:1" />)
    expect(snippet()).toBe("curl -G 'http://localhost:3000/api/v1/graphs/g1/query/gwas_by_trait.csv' \\\n  --data-urlencode 'trait_id=mondo:1'")
    fireEvent.change(paramInput('trait_id'), { target: { value: 'mondo:2' } })
    expect(snippet()).toContain("'trait_id=mondo:2'")
    fireEvent.click(screen.getByRole('button', { name: 'R' }))
    expect(snippet()).toContain('gwas_by_trait <- function(trait_id)')
  })

  it('runs the query with the current parameters and shows the response', async () => {
    render(<QueryTemplateExample id="gwas_by_trait" graph="g1" trait_id="mondo:1" />)
    await waitFor(() => expect(screen.getByRole('button', { name: 'Try it' })).toBeEnabled())
    fireEvent.click(screen.getByRole('button', { name: 'Try it' }))
    expect(fetchMock).toHaveBeenCalledWith('http://localhost:3000/api/v1/graphs/g1/query/gwas_by_trait?trait_id=mondo%3A1')
    expect(await screen.findByText('Response')).toBeInTheDocument()
    expect(document.querySelector('pre.whitespace-pre-wrap')).toHaveTextContent('"study": "GCST1"')
  })

  it('does not check availability without a graph and reports request errors', async () => {
    render(<QueryTemplateExample id="q" />)
    expect(fetchMock).not.toHaveBeenCalled()
    fetchMock.mockRejectedValue(new Error('boom'))
    fireEvent.click(screen.getByRole('button', { name: 'Try it' }))
    expect(await screen.findByText('boom')).toBeInTheDocument()
  })
})
