import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, act, cleanup } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import NodeSelectorBox from './NodeSelectorBox'
import GraphNodeRef from '../model/GraphNodeRef'
import { Page } from '../app/api'

const api = vi.hoisted(() => ({ getPaginated: vi.fn() }))

vi.mock('../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, getPaginated: api.getPaginated }
})

const nodes = [
  { 'grebi:nodeId': 'n-psoriasis', 'grebi:curie': 'mondo:0005083', 'grebi:name': ['psoriasis'], 'grebi:type': ['biolink:Disease'], 'grebi:datasources': ['MONDO'] },
  { 'grebi:nodeId': 'n-psa', 'grebi:curie': 'mondo:0011849', 'grebi:name': ['psoriatic arthritis'], 'grebi:type': ['biolink:Disease'], 'grebi:datasources': ['MONDO'] },
]

type Props = Parameters<typeof NodeSelectorBox>[0]

function renderBox(props: Partial<Props> = {}) {
  const onNodeSelect = vi.fn()
  const onClear = vi.fn()
  const utils = render(
    <MemoryRouter>
      <NodeSelectorBox graph="g" onNodeSelect={onNodeSelect} onClear={onClear} {...props} />
    </MemoryRouter>
  )
  return { onNodeSelect, onClear, ...utils, input: () => screen.getByRole('textbox') }
}

async function tick(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

beforeEach(() => {
  vi.useFakeTimers()
  api.getPaginated.mockReset()
  api.getPaginated.mockImplementation(async () => new Page(0, nodes.length, 1, nodes.length, nodes, new Map()))
})

afterEach(() => {
  cleanup()
  vi.clearAllTimers()
  vi.useRealTimers()
})

describe('NodeSelectorBox', () => {
  it('searches after the debounce, lists the hits and hands the chosen node back', async () => {
    const { onNodeSelect, input, container } = renderBox({
      additionalParams: new URLSearchParams({ 'grebi:type': 'biolink:Disease' }),
    })
    expect(input()).toHaveAttribute('placeholder', 'Type to search…')

    fireEvent.focus(input())
    fireEvent.change(input(), { target: { value: 'psor' } })
    expect(container.querySelector('.spinner-default')).not.toBeNull()
    expect(api.getPaginated).not.toHaveBeenCalled()

    await tick(300)
    expect(api.getPaginated).toHaveBeenCalledTimes(1)
    expect(api.getPaginated.mock.calls[0][0]).toBe(
      'api/v1/graphs/g/search?q=psor&resolve=false&size=5&lang=en&grebi%3Atype=biolink%3ADisease'
    )
    expect(container.querySelector('.spinner-default')).toBeNull()

    const items = screen.getAllByRole('listitem')
    expect(items.map((li) => li.textContent)).toEqual(['psoriasisDiseaseMONDO', 'psoriatic arthritisDiseaseMONDO'])

    fireEvent.click(items[1])
    expect(onNodeSelect).toHaveBeenCalledTimes(1)
    expect(onNodeSelect.mock.calls[0][0]).toBeInstanceOf(GraphNodeRef)
    expect(onNodeSelect.mock.calls[0][0].getNodeId()).toBe('n-psa')
    // the box is handed back empty; what shows next is the parent's selectedNode
    expect(input()).toHaveValue('')
    expect(screen.queryByRole('listitem')).toBeNull()
  })

  it('collapses rapid typing into one request for the latest text', async () => {
    const { input } = renderBox()
    fireEvent.change(input(), { target: { value: 'p' } })
    await tick(200)
    fireEvent.change(input(), { target: { value: 'ps' } })
    await tick(200)
    expect(api.getPaginated).not.toHaveBeenCalled()
    await tick(100)
    expect(api.getPaginated).toHaveBeenCalledTimes(1)
    expect(api.getPaginated.mock.calls[0][0]).toBe('api/v1/graphs/g/search?q=ps&resolve=false&size=5&lang=en')
  })

  it('a blank query neither searches nor lists anything', async () => {
    const { input, container } = renderBox()
    fireEvent.change(input(), { target: { value: '   ' } })
    await tick(300)
    expect(api.getPaginated).not.toHaveBeenCalled()
    expect(screen.queryByRole('listitem')).toBeNull()
    expect(container.querySelector('.spinner-default')).toBeNull()
  })

  it('shows the selected node and clears it through the clear button', () => {
    const { onClear, input } = renderBox({ selectedNode: new GraphNodeRef(nodes[0]) })
    expect(input()).toHaveValue('psoriasis')
    expect(input()).toHaveAttribute('placeholder', '')
    fireEvent.click(screen.getByTestId('CloseIcon').closest('button')!)
    expect(onClear).toHaveBeenCalledTimes(1)
  })

  it('focusing or typing over a selected node clears the selection first', () => {
    const { onClear, input } = renderBox({ selectedNode: new GraphNodeRef(nodes[0]) })
    fireEvent.focus(input())
    expect(onClear).toHaveBeenCalledTimes(1)
    fireEvent.change(input(), { target: { value: 'ecz' } })
    expect(onClear).toHaveBeenCalledTimes(2)
    expect(input()).toHaveValue('ecz')
  })

  it('arrow keys move the highlight and Enter picks it', async () => {
    const { onNodeSelect, input } = renderBox()
    fireEvent.focus(input())
    fireEvent.change(input(), { target: { value: 'psor' } })
    await tick(300)

    fireEvent.keyDown(input(), { key: 'ArrowDown' })
    fireEvent.keyDown(input(), { key: 'ArrowDown' })
    expect(screen.getAllByRole('listitem')[1]).toHaveClass('bg-link-light')
    fireEvent.keyDown(input(), { key: 'ArrowUp' })
    expect(screen.getAllByRole('listitem')[0]).toHaveClass('bg-link-light')
    expect(screen.getAllByRole('listitem')[1]).not.toHaveClass('bg-link-light')

    fireEvent.keyDown(input(), { key: 'Enter' })
    expect(onNodeSelect).toHaveBeenCalledTimes(1)
    expect(onNodeSelect.mock.calls[0][0].getNodeId()).toBe('n-psoriasis')
  })

  it('Enter with nothing highlighted selects nothing', async () => {
    const { onNodeSelect, input } = renderBox()
    fireEvent.change(input(), { target: { value: 'psor' } })
    await tick(300)
    fireEvent.keyDown(input(), { key: 'Enter' })
    expect(onNodeSelect).not.toHaveBeenCalled()
  })
})
