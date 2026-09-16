import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, act, cleanup } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import CyclingQuestions from './CyclingQuestions'

const api = vi.hoisted(() => ({ get: vi.fn(), getPaginated: vi.fn() }))

vi.mock('../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, get: api.get, getPaginated: api.getPaginated }
})

function LocationDisplay() {
  const loc = useLocation()
  return <div data-testid="location">{loc.pathname + loc.search}</div>
}

// Single-parameter templates with titled examples display without any lookups.
const templates: any[] = [
  {
    id: 'q_disease',
    question: 'What GWAS [studies]{study} report {disease_id}?',
    params: [{ param_id: 'disease_id', param_name: 'Disease', param_type: 'SourceId' }],
    result_columns: [],
    examples: [
      { title: 'psoriasis', params: { disease_id: 'mondo:0005083' } },
      { title: 'asthma', params: { disease_id: 'mondo:0004979' } },
    ],
  },
  {
    id: 'q_gene',
    question: 'Which [variants]{snp} are near {gene_id}?',
    params: [{ param_id: 'gene_id', param_name: 'Gene', param_type: 'SourceId' }],
    result_columns: [],
    examples: [{ title: 'BRCA2', params: { gene_id: 'hgnc:1101' } }],
  },
  { id: 'q_silent', question: '', params: [], result_columns: [], examples: [] },
]

// Let the mocked template load, or a transition, settle.
async function flush(ms = 0) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

type Props = Parameters<typeof CyclingQuestions>[0]

function renderCycling(props: Partial<Props> = {}) {
  const utils = render(
    <MemoryRouter initialEntries={['/']}>
      <CyclingQuestions graph="g" {...props} />
      <LocationDisplay />
    </MemoryRouter>
  )
  return { ...utils, question: () => utils.container.querySelector('.text-center')?.textContent ?? '' }
}

beforeEach(() => {
  vi.useFakeTimers()
  api.get.mockReset()
  api.getPaginated.mockReset()
  api.get.mockResolvedValue(templates)
})

afterEach(() => {
  cleanup()
  vi.clearAllTimers()
  vi.useRealTimers()
})

describe('CyclingQuestions', () => {
  it('loads the templates that have a question and shows the first with its example', async () => {
    const { container, question } = renderCycling()
    expect(container.querySelector('.text-center')).toBeNull()
    await flush()
    expect(api.get).toHaveBeenCalledWith('api/v1/graphs/g/query_templates')
    expect(question()).toBe('What GWAS studies report psoriasis?')
    // one dot per template with a question, plus the two arrows
    expect(screen.getAllByRole('button')).toHaveLength(4)
    expect(api.getPaginated).not.toHaveBeenCalled()
  })

  it('the arrows step through the questions, rotating the examples of a template each time it is left', async () => {
    const { question } = renderCycling()
    await flush()
    fireEvent.click(screen.getByTitle('Next question'))
    await flush()
    expect(question()).toBe('Which variants are near BRCA2?')
    fireEvent.click(screen.getByTitle('Next question'))
    await flush()
    expect(question()).toBe('What GWAS studies report asthma?')
    fireEvent.click(screen.getByTitle('Previous question'))
    await flush()
    expect(question()).toBe('Which variants are near BRCA2?')
  })

  it('the dots jump straight to a question', async () => {
    const { question } = renderCycling()
    await flush()
    const dots = screen.getAllByRole('button').filter((b) => !b.title)
    expect(dots).toHaveLength(2)
    expect(dots[0]).toHaveClass('bg-blue-500')
    fireEvent.click(dots[1])
    await flush()
    expect(question()).toBe('Which variants are near BRCA2?')
    expect(dots[1]).toHaveClass('bg-blue-500')
    expect(dots[0]).not.toHaveClass('bg-blue-500')
  })

  it('auto-cycles every six seconds unless autoPlay is off', async () => {
    const { question, rerender } = renderCycling()
    await flush()
    await flush(5999)
    expect(question()).toBe('What GWAS studies report psoriasis?')
    await flush(1)
    expect(question()).toBe('Which variants are near BRCA2?')

    rerender(
      <MemoryRouter initialEntries={['/']}>
        <CyclingQuestions graph="g" autoPlay={false} />
        <LocationDisplay />
      </MemoryRouter>
    )
    await flush(12000)
    expect(question()).toBe('Which variants are near BRCA2?')
  })

  it('clicking the question opens the query with the example and stops cycling', async () => {
    const { question } = renderCycling()
    await flush()
    fireEvent.click(screen.getByText('psoriasis'))
    expect(screen.getByTestId('location')).toHaveTextContent('/graphs/g/queries/q_disease?disease_id=mondo%3A0005083')
    await flush(12000)
    expect(question()).toBe('What GWAS studies report psoriasis?')
  })

  it('reports the visible and upcoming example ids to the parent', async () => {
    const onVisibleSourceIdsChange = vi.fn()
    renderCycling({ onVisibleSourceIdsChange })
    await flush()
    expect(onVisibleSourceIdsChange).toHaveBeenLastCalledWith('mondo:0005083', 'hgnc:1101')
    fireEvent.click(screen.getByTitle('Next question'))
    await flush()
    expect(onVisibleSourceIdsChange).toHaveBeenLastCalledWith('hgnc:1101', 'mondo:0004979')
  })

  it('arrow keys step through the questions while the widget has focus', async () => {
    const { question } = renderCycling()
    await flush()
    screen.getByTitle('Next question').focus()
    fireEvent.keyDown(window, { key: 'ArrowRight' })
    await flush()
    expect(question()).toBe('Which variants are near BRCA2?')
    fireEvent.keyDown(window, { key: 'ArrowLeft' })
    await flush()
    expect(question()).toBe('What GWAS studies report asthma?')
  })

  it('renders nothing when no template has a question', async () => {
    api.get.mockResolvedValue([templates[2]])
    const { container } = renderCycling()
    await flush()
    expect(container.querySelector('.text-center')).toBeNull()
    expect(screen.queryByTitle('Next question')).toBeNull()
  })
})
