import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import GraphPicker from './GraphPicker'
import { get } from '../app/api'

vi.mock('../app/api', () => ({ get: vi.fn() }))
const getMock = vi.mocked(get)

beforeEach(() => {
  // MUI warns that the value is out of range until the graph list has loaded
  vi.spyOn(console, 'warn').mockImplementation(() => {})
  getMock.mockReset()
  getMock.mockImplementation(async (path: string) => {
    if (path === 'api/v1/graphs') return ['ebi_monarch_xspecies', 'test_gwas']
    if (path === 'api/v1/stats') return { ebi_monarch_xspecies: { num_nodes: 1234567, num_edges: 89 } }
    throw new Error('unexpected ' + path)
  })
})

afterEach(() => vi.restoreAllMocks())

async function openPicker(setGraph = vi.fn()) {
  render(<GraphPicker graph="test_gwas" setGraph={setGraph} />)
  fireEvent.mouseDown(screen.getByRole('combobox'))
  await screen.findByRole('option', { name: /ebi_monarch_xspecies/ })
  return setGraph
}

describe('GraphPicker', () => {
  it('lists the graphs from the API with node and edge counts where known', async () => {
    await openPicker()
    const options = screen.getAllByRole('option').map(o => o.textContent)
    expect(options).toHaveLength(2)
    expect(options[0]).toBe(`ebi_monarch_xspecies${(1234567).toLocaleString()} nodes, ${(89).toLocaleString()} edges`)
    expect(options[1]).toBe('test_gwas')
    expect(getMock).toHaveBeenCalledWith('api/v1/graphs')
    expect(getMock).toHaveBeenCalledWith('api/v1/stats')
  })

  it('reports the chosen graph', async () => {
    const setGraph = await openPicker()
    fireEvent.click(screen.getByRole('option', { name: /ebi_monarch_xspecies/ }))
    expect(setGraph).toHaveBeenCalledWith('ebi_monarch_xspecies')
  })
})
