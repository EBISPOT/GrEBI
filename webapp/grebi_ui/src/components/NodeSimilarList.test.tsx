import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import NodeSimilarList from './NodeSimilarList'
import GraphNodeRef from '../model/GraphNodeRef'
import encodeNodeId from '../encodeNodeId'

const api = vi.hoisted(() => ({ get: vi.fn() }))

vi.mock('../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, get: api.get }
})

const node = new GraphNodeRef({ 'grebi:nodeId': 'n-psoriasis', 'grebi:name': ['psoriasis'] })

// the postgres-backed endpoint returns flat rows with a distance
const pgResults = [
  { nodeId: 'n-psa', name: ['psoriatic arthritis'], distance: 0.1, datasources: ['MONDO'], type: ['biolink:Disease'] },
  { nodeId: 'n-eczema', name: ['eczema'], distance: 0.25 },
  { nodeId: 'n-nodist', name: ['no distance'] },
]

function renderList(model?: string) {
  return render(
    <MemoryRouter>
      <NodeSimilarList graph="g" node={node} model={model} />
    </MemoryRouter>
  )
}

beforeEach(() => {
  api.get.mockReset()
  api.get.mockResolvedValue(pgResults)
})

describe('NodeSimilarList', () => {
  it('shows a spinner, then the neighbours scored as one minus their distance', async () => {
    const { container } = renderList()
    expect(container.querySelector('.spinner-default')).not.toBeNull()
    expect(api.get).toHaveBeenCalledWith(`api/v1/graphs/g/nodes/${encodeNodeId('n-psoriasis')}/similar?n=20`)

    await screen.findByText('psoriatic arthritis')
    expect(container.querySelector('.spinner-default')).toBeNull()
    expect(screen.getByText('0.9000')).toBeInTheDocument()
    expect(screen.getByText('0.7500')).toBeInTheDocument()
    // no distance at all scores zero rather than breaking the list
    expect(screen.getByText('0.0000')).toBeInTheDocument()

    const link = screen.getByText('psoriatic arthritis').closest('a')!
    expect(link).toHaveAttribute('href', `/graphs/g/nodes/${encodeNodeId('n-psa')}`)
    expect(screen.getAllByRole('row')).toHaveLength(1 + pgResults.length)
  })

  it('accepts the older node/score shape', async () => {
    api.get.mockResolvedValue([{ node: { 'grebi:nodeId': 'n-x', 'grebi:name': ['older format'] }, score: 0.5 }])
    renderList()
    expect(await screen.findByText('older format')).toBeInTheDocument()
    expect(screen.getByText('0.5000')).toBeInTheDocument()
  })

  it('explains when there is nothing similar', async () => {
    api.get.mockResolvedValue([])
    renderList()
    expect(await screen.findByText(/No similar nodes found/)).toBeInTheDocument()
    expect(screen.queryByRole('table')).toBeNull()
  })

  it('passes the model along and re-queries when it changes', async () => {
    const { rerender } = renderList('minilm')
    await screen.findByText('psoriatic arthritis')
    expect(api.get).toHaveBeenLastCalledWith(`api/v1/graphs/g/nodes/${encodeNodeId('n-psoriasis')}/similar?n=20&model=minilm`)

    rerender(
      <MemoryRouter>
        <NodeSimilarList graph="g" node={node} />
      </MemoryRouter>
    )
    await screen.findByText('psoriatic arthritis')
    expect(api.get).toHaveBeenCalledTimes(2)
    expect(api.get).toHaveBeenLastCalledWith(`api/v1/graphs/g/nodes/${encodeNodeId('n-psoriasis')}/similar?n=20`)
  })
})
