import { describe, it, expect } from 'vitest'
import { render } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import QueryQuestion from './QueryQuestion'

// One SourceId parameter with a titled example: the read-only rendering needs
// no API call, the example title stands in for the parameter.
const template: any = {
  id: 'gwas_studies_by_cell_type',
  title: 'Search GWAS Catalog studies by cell type',
  question: 'What GWAS [studies]{study} report traits affecting cells of type {cell_type_id}?',
  graphs: ['ebi_monarch_xspecies'],
  params: [{ param_id: 'cell_type_id', param_name: 'Cell Type', param_type: 'SourceId' }],
  result_columns: [{ column_id: 'study', column_type: 'GraphNodeId' }],
  examples: [{ title: 'leukocyte', params: { cell_type_id: 'cl:0000738' } }],
}

describe('QueryQuestion', () => {
  it('renders the question with the example filled in and result references as text', () => {
    const { container } = render(
      <MemoryRouter>
        <QueryQuestion graph="ebi_monarch_xspecies" template={template} exampleIndex={0} readOnly />
      </MemoryRouter>
    )
    expect(container.textContent).toContain('What GWAS studies report traits affecting cells of type leukocyte?')
  })
})
