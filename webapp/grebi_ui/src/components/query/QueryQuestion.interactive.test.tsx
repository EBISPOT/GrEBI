import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, act, cleanup } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import QueryQuestion from './QueryQuestion'
import { Page } from '../../app/api'

// The editable question: inline autocompletes for SourceId parameters and
// plain inputs for the rest. The read-only rendering is covered in
// QueryQuestion.test.tsx.

const api = vi.hoisted(() => ({ getPaginated: vi.fn() }))

vi.mock('../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, getPaginated: api.getPaginated }
})

function LocationDisplay() {
  const loc = useLocation()
  return <div data-testid="location">{loc.pathname + loc.search}</div>
}

const nodes = [
  { 'grebi:nodeId': 'n-psoriasis', 'grebi:curie': 'mondo:0005083', 'grebi:name': ['psoriasis'], 'grebi:type': ['biolink:Disease'], 'grebi:datasources': ['MONDO'] },
  { 'grebi:nodeId': 'n-psa', 'grebi:curie': 'mondo:0011849', 'grebi:name': ['psoriatic arthritis'], 'grebi:type': ['biolink:Disease'], 'grebi:datasources': ['MONDO'] },
]

const diseaseTemplate: any = {
  id: 'gwas_studies_by_disease',
  question: 'What GWAS [studies]{study} report {disease_id}?',
  params: [{ param_id: 'disease_id', param_name: 'Disease', param_type: 'SourceId', values_with_type: 'biolink:Disease' }],
  result_columns: [{ column_id: 'study', column_type: 'GraphNodeId' }],
  examples: [{ title: 'psoriasis', params: { disease_id: 'mondo:0005083' } }],
}

const textTemplate: any = {
  id: 'genes_on_chromosome',
  question: 'Which [genes]{gene} on chromosome {chrom} score above {min_score}?',
  params: [
    { param_id: 'chrom', param_name: 'Chromosome', param_type: 'string' },
    { param_id: 'min_score', param_name: 'Minimum score', param_type: 'float' },
  ],
  result_columns: [{ column_id: 'gene', column_type: 'GraphNodeId' }],
  examples: [{ title: 'chromosome 7', params: { chrom: '7', min_score: '0.5' } }],
}

type Props = Parameters<typeof QueryQuestion>[0]

function renderQuestion(props: Partial<Props> = {}) {
  const onAllParamsFilled = vi.fn()
  render(
    <MemoryRouter initialEntries={['/']}>
      <QueryQuestion graph="g" template={diseaseTemplate} exampleIndex={0} onAllParamsFilled={onAllParamsFilled} {...props} />
      <LocationDisplay />
    </MemoryRouter>
  )
  return { onAllParamsFilled }
}

// Advance the debounce and let the mocked response land.
async function tick(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

function typeInto(input: HTMLElement, value: string) {
  fireEvent.focus(input)
  fireEvent.change(input, { target: { value } })
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

describe('QueryQuestion (interactive)', () => {
  it('renders an input per parameter with the example as placeholder and result refs as bold text', () => {
    renderQuestion()
    expect(screen.getByPlaceholderText('psoriasis')).toHaveValue('')
    expect(screen.getByText('studies')).toHaveClass('font-semibold')
    expect(screen.queryByTestId('PlayArrowIcon')).toBeNull()
    expect(api.getPaginated).not.toHaveBeenCalled()
  })

  it('text parameters use example values as placeholders and enable the go button once all are filled', () => {
    renderQuestion({ template: textTemplate })
    const chrom = screen.getByPlaceholderText('7')
    const score = screen.getByPlaceholderText('0.5')
    expect(score).toHaveAttribute('type', 'number')
    expect(screen.queryByTestId('PlayArrowIcon')).toBeNull()

    fireEvent.change(chrom, { target: { value: 'X' } })
    expect(screen.queryByTestId('PlayArrowIcon')).toBeNull()
    fireEvent.change(score, { target: { value: '0.9' } })
    expect(screen.getByTestId('PlayArrowIcon')).toBeInTheDocument()
    expect(api.getPaginated).not.toHaveBeenCalled()

    fireEvent.keyDown(score, { key: 'Enter' })
    expect(screen.getByTestId('location')).toHaveTextContent(
      '/graphs/g/queries/genes_on_chromosome?chrom=X&min_score=0.9'
    )
  })

  it('fetches suggestions after the debounce, filtered to the parameter type', async () => {
    renderQuestion()
    const input = screen.getByPlaceholderText('psoriasis')
    typeInto(input, 'psor')
    expect(api.getPaginated).not.toHaveBeenCalled()
    await tick(299)
    expect(api.getPaginated).not.toHaveBeenCalled()
    await tick(1)
    expect(api.getPaginated).toHaveBeenCalledTimes(1)

    const path: string = api.getPaginated.mock.calls[0][0]
    expect(path.startsWith('api/v1/graphs/g/search?')).toBe(true)
    const qs = new URLSearchParams(path.split('?')[1])
    expect(qs.get('q')).toBe('psor')
    expect(qs.get('size')).toBe('5')
    expect(qs.get('grebi:type')).toBe('biolink:Disease')

    const items = screen.getAllByRole('listitem')
    expect(items.map((li) => li.textContent)).toEqual(['psoriasisDiseaseMONDO', 'psoriatic arthritisDiseaseMONDO'])
  })

  it('selecting a suggestion fills the parameter, reports it and navigates to the query', async () => {
    const { onAllParamsFilled } = renderQuestion()
    const input = screen.getByPlaceholderText('psoriasis')
    typeInto(input, 'psor')
    await tick(300)

    fireEvent.click(screen.getByText('psoriasis'))
    // the curie, not the internal node id, identifies the node in the query
    expect(onAllParamsFilled).toHaveBeenCalledWith({ disease_id: 'mondo:0005083' })
    expect(screen.getByTestId('location')).toHaveTextContent(
      '/graphs/g/queries/gwas_studies_by_disease?disease_id=mondo%3A0005083'
    )
    expect(input).toHaveValue('psoriasis')
    expect(screen.queryByRole('list')).toBeNull()
    expect(screen.queryByTestId('PlayArrowIcon')).toBeNull()
  })

  it('without auto-navigation the go button appears and runs the query on click', async () => {
    const { onAllParamsFilled } = renderQuestion({ autoNavigate: false })
    typeInto(screen.getByPlaceholderText('psoriasis'), 'psor')
    await tick(300)

    fireEvent.click(screen.getByText('psoriatic arthritis'))
    expect(onAllParamsFilled).toHaveBeenCalledWith({ disease_id: 'mondo:0011849' })
    expect(screen.getByTestId('location')).toHaveTextContent(/^\/$/)

    fireEvent.click(screen.getByTestId('PlayArrowIcon').closest('button')!)
    expect(screen.getByTestId('location')).toHaveTextContent(
      '/graphs/g/queries/gwas_studies_by_disease?disease_id=mondo%3A0011849'
    )
  })

  it('arrow keys move the highlight and Enter picks the highlighted suggestion', async () => {
    renderQuestion()
    const input = screen.getByPlaceholderText('psoriasis')
    typeInto(input, 'psor')
    await tick(300)

    fireEvent.keyDown(input, { key: 'ArrowDown' })
    fireEvent.keyDown(input, { key: 'ArrowDown' })
    expect(screen.getAllByRole('listitem')[0]).not.toHaveClass('bg-blue-50')
    expect(screen.getAllByRole('listitem')[1]).toHaveClass('bg-blue-50')
    fireEvent.keyDown(input, { key: 'ArrowUp' })
    expect(screen.getAllByRole('listitem')[0]).toHaveClass('bg-blue-50')

    fireEvent.keyDown(input, { key: 'Enter' })
    expect(screen.getByTestId('location')).toHaveTextContent('disease_id=mondo%3A0005083')
  })

  it('collapses rapid typing into a single request for the latest text', async () => {
    renderQuestion()
    const input = screen.getByPlaceholderText('psoriasis')
    typeInto(input, 'p')
    await tick(200)
    fireEvent.change(input, { target: { value: 'ps' } })
    await tick(200)
    expect(api.getPaginated).not.toHaveBeenCalled()
    await tick(100)
    expect(api.getPaginated).toHaveBeenCalledTimes(1)
    expect(api.getPaginated.mock.calls[0][0]).toContain('q=ps&')
  })

  it('shows a placeholder the template does not declare in red', () => {
    renderQuestion({ template: { ...diseaseTemplate, question: 'Studies of {disease_id} in {population}?' } })
    expect(screen.getByText('{population}')).toHaveClass('text-red-500')
    expect(screen.getByPlaceholderText('psoriasis')).toBeInTheDocument()
  })
})
