import { describe, it, expect, vi } from 'vitest'
import { render, screen, within, fireEvent } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import QueryTable from './QueryTable'

// The read-only example questions never need the API for single-parameter
// templates with titled examples, but keep the network out regardless.
vi.mock('../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, getPaginated: vi.fn(async () => new mod.Page(0, 0, 0, 0, [], new Map())) }
})

function LocationDisplay() {
  const loc = useLocation()
  return <div data-testid="location">{loc.pathname + loc.search}</div>
}

const templates: any[] = [
  {
    id: 'gwas_studies_by_disease',
    title: 'GWAS studies by disease',
    question: 'What GWAS [studies]{study} report {disease_id}?',
    graphs: ['g'],
    params: [{ param_id: 'disease_id', param_name: 'Disease', param_type: 'SourceId' }],
    result_columns: [{ column_id: 'study', column_type: 'GraphNodeId' }],
    examples: [{ title: 'psoriasis', params: { disease_id: 'mondo:0005083' } }],
  },
  {
    id: 'all_genes',
    title: 'All genes',
    question: 'List every [gene]{gene}',
    graphs: ['g'],
    params: [],
    result_columns: [{ column_id: 'gene', column_type: 'GraphNodeId' }],
    examples: [],
  },
]

type Props = Parameters<typeof QueryTable>[0]

function renderTable(overrides: Partial<Props> = {}) {
  const props: Props = {
    graph: 'g',
    queries: templates,
    availableInputs: ['disease_id', 'gene_id'],
    availableOutputs: ['gene', 'study'],
    selectedInputs: new Set<string>(),
    selectedOutputs: new Set<string>(),
    onInputsChange: vi.fn(),
    onOutputsChange: vi.fn(),
    ...overrides,
  }
  render(
    <MemoryRouter initialEntries={['/graphs/g/queries']}>
      <QueryTable {...props} />
      <LocationDisplay />
    </MemoryRouter>
  )
  return props
}

const filterButton = (heading: string) =>
  within(screen.getByText(heading).closest('th')!).getByRole('button')

describe('QueryTable', () => {
  it('shows a spinner while the templates are still loading', () => {
    renderTable({ queries: null })
    expect(screen.getByRole('progressbar')).toBeInTheDocument()
    expect(screen.queryByRole('table')).toBeNull()
  })

  it('says so when no template passes the filters', () => {
    renderTable({ queries: [] })
    expect(screen.getByText('No queries match the current filters.')).toBeInTheDocument()
  })

  it('lists each template with its id, input and output badges and the example question', () => {
    renderTable()
    const row = screen.getByText('gwas_studies_by_disease').closest('tr')!
    expect(within(row).getByText('disease_id')).toBeInTheDocument()
    expect(within(row).getByText('study')).toBeInTheDocument()
    expect(row.textContent).toContain('What GWAS studies report psoriasis?')

    const genes = screen.getByText('all_genes').closest('tr')!
    expect(within(genes).getByText('gene', { selector: 'code' })).toBeInTheDocument()
    expect(genes.textContent).toContain('List every gene')
  })

  it('clicking a row opens the query with its first example, or bare when there is none', () => {
    renderTable()
    fireEvent.click(screen.getByText('gwas_studies_by_disease'))
    expect(screen.getByTestId('location')).toHaveTextContent(
      '/graphs/g/queries/gwas_studies_by_disease?disease_id=mondo%3A0005083'
    )
    fireEvent.click(screen.getByText('all_genes'))
    expect(screen.getByTestId('location')).toHaveTextContent('/graphs/g/queries/all_genes')
    expect(screen.getByTestId('location').textContent).not.toContain('?')
  })

  it('the inputs filter opens a popover whose checkboxes report the new selection', () => {
    const props = renderTable()
    fireEvent.click(filterButton('Inputs'))
    expect(screen.getByText('Filter inputs')).toBeInTheDocument()
    // nothing selected yet, so there is nothing to clear
    expect(screen.queryByRole('button', { name: 'Clear' })).toBeNull()

    fireEvent.click(screen.getByLabelText('gene_id'))
    expect(props.onInputsChange).toHaveBeenLastCalledWith(new Set(['gene_id']))
    expect(props.onOutputsChange).not.toHaveBeenCalled()
  })

  it('the outputs filter shows the selection count and can clear it', () => {
    const props = renderTable({ selectedOutputs: new Set(['study']) })
    expect(within(screen.getByText('Outputs').closest('th')!).getByText('1')).toBeInTheDocument()

    fireEvent.click(filterButton('Outputs'))
    expect(screen.getByText('Filter outputs')).toBeInTheDocument()
    expect(screen.getByLabelText('study')).toBeChecked()
    expect(screen.getByLabelText('gene')).not.toBeChecked()

    fireEvent.click(screen.getByLabelText('study'))
    expect(props.onOutputsChange).toHaveBeenLastCalledWith(new Set())

    fireEvent.click(screen.getByRole('button', { name: 'Clear' }))
    expect(props.onOutputsChange).toHaveBeenLastCalledWith(new Set())
    expect(props.onOutputsChange).toHaveBeenCalledTimes(2)
  })
})
