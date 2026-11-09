import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, within, cleanup } from '@testing-library/react'
import GraphViewControls, { EdgeTypeGroup } from './GraphViewControls'

/** A category of edge types, each with ten times its place in the list as its count */
const group = (label: string, edgeTypes: string[]): EdgeTypeGroup => ({
  category: { id: label.toLowerCase(), label },
  edgeTypes: edgeTypes.map((edgeType, i) => ({ edgeType, count: (i + 1) * 10 })),
})

function renderControls(overrides: Partial<React.ComponentProps<typeof GraphViewControls>> = {}) {
  const props = {
    datasources: ['OLS.mondo', 'gwas'],
    dsEnabled: ['OLS.mondo', 'gwas'],
    setDsEnabled: vi.fn(),
    onMouseoverDs: vi.fn(),
    onMouseoutDs: vi.fn(),
    edgeTypeGroups: [group('Other', ['has_part', 'is_a', 'regulates'])],
    hiddenEdgeTypes: new Set<string>(),
    onToggleEdgeType: vi.fn(),
    onShowAllEdgeTypes: vi.fn(),
    onHideAllEdgeTypes: vi.fn(),
    onMouseoverEdgeType: vi.fn(),
    onMouseoutEdgeType: vi.fn(),
    ...overrides,
  }
  render(<GraphViewControls {...props} />)
  return props
}

const dsCheckbox = (title: string) => within(screen.getByTitle(title).parentElement!).getByRole('checkbox')

describe('GraphViewControls', () => {
  it('lists datasources (ontologies last, by short name) and reports toggles', () => {
    const props = renderControls()
    // 'gwas' sorts before the ontology, whose label is the part after the dot
    const tags = screen.getAllByText(/^(gwas|mondo)$/).map((el) => el.textContent)
    expect(tags).toEqual(['gwas', 'mondo'])

    fireEvent.click(dsCheckbox('gwas'))
    expect(props.setDsEnabled).toHaveBeenCalledWith(['OLS.mondo'])
  })

  it('shows how many edge types are visible and hides the section when there are none', () => {
    const { unmount } = render(
      <GraphViewControls
        datasources={['a']} dsEnabled={['a']} setDsEnabled={vi.fn()}
        edgeTypeGroups={[group('Other', ['x', 'y', 'z'])]} hiddenEdgeTypes={new Set(['z'])}
        onToggleEdgeType={vi.fn()} onShowAllEdgeTypes={vi.fn()} onHideAllEdgeTypes={vi.fn()}
      />
    )
    expect(screen.getByText('Edge Types (2/3)')).toBeInTheDocument()
    unmount()

    renderControls({ edgeTypeGroups: [] })
    expect(screen.queryByText(/Edge Types/)).toBeNull()
  })

  it('reflects hidden edge types in the checkboxes and calls back on toggle', () => {
    const props = renderControls({ hiddenEdgeTypes: new Set(['is_a']) })
    expect(screen.getByLabelText('has_part')).toBeChecked()
    expect(screen.getByLabelText('is_a')).not.toBeChecked()
    fireEvent.click(screen.getByLabelText('is_a'))
    expect(props.onToggleEdgeType).toHaveBeenCalledWith('is_a')
  })

  it('disables All when everything is visible and None when everything is hidden', () => {
    let props = renderControls()
    // the datasource selector has its own All/None pair; the edge type pair comes second
    let [, allEdges] = screen.getAllByRole('button', { name: 'All' })
    let [, noneEdges] = screen.getAllByRole('button', { name: 'None' })
    expect(allEdges).toBeDisabled()
    expect(noneEdges).toBeEnabled()
    fireEvent.click(noneEdges)
    expect(props.onHideAllEdgeTypes).toHaveBeenCalled()

    cleanup()
    props = renderControls({ hiddenEdgeTypes: new Set(['has_part', 'is_a', 'regulates']) })
    ;[, allEdges] = screen.getAllByRole('button', { name: 'All' })
    ;[, noneEdges] = screen.getAllByRole('button', { name: 'None' })
    expect(allEdges).toBeEnabled()
    expect(noneEdges).toBeDisabled()
    fireEvent.click(allEdges)
    expect(props.onShowAllEdgeTypes).toHaveBeenCalled()
  })

  it('only offers All/None for edge types when there is more than one', () => {
    renderControls({ datasources: ['a'], dsEnabled: ['a'], edgeTypeGroups: [group('Other', ['only'])] })
    expect(screen.queryByRole('button', { name: 'All' })).toBeNull()
    expect(screen.getByLabelText('only')).toBeInTheDocument()
  })

  it('forwards hover events for datasources and edge types', () => {
    const props = renderControls()
    fireEvent.mouseEnter(screen.getByTitle('gwas'))
    expect(props.onMouseoverDs).toHaveBeenCalledWith('gwas')
    fireEvent.mouseLeave(screen.getByTitle('gwas'))
    expect(props.onMouseoutDs).toHaveBeenCalledWith('gwas')

    fireEvent.mouseEnter(screen.getByText('regulates'))
    expect(props.onMouseoverEdgeType).toHaveBeenCalledWith('regulates')
    fireEvent.mouseLeave(screen.getByText('regulates'))
    expect(props.onMouseoutEdgeType).toHaveBeenCalledWith('regulates')
  })

  it('lists edge types under their categories, with their counts, and shows or hides a category at once', () => {
    const props = renderControls({
      edgeTypeGroups: [group('Hierarchy', ['biolink:subclass_of', 'ols:hierarchicalParent']), group('Identifiers and metadata', ['rdf:type'])],
      hiddenEdgeTypes: new Set(['ols:hierarchicalParent', 'rdf:type']),
      onSetEdgeTypesHidden: vi.fn(),
    })
    expect(screen.getByText('Edge Types (1/3)')).toBeInTheDocument()
    expect(screen.getByText('Hierarchy')).toBeInTheDocument()
    expect(screen.getByText('20')).toBeInTheDocument()

    // partly shown: the category's box is indeterminate, and ticking it shows them all
    const hierarchy = screen.getByLabelText('Hierarchy')
    expect(hierarchy).toHaveAttribute('data-indeterminate', 'true')
    fireEvent.click(hierarchy)
    expect(props.onSetEdgeTypesHidden).toHaveBeenCalledWith(['biolink:subclass_of', 'ols:hierarchicalParent'], false)

    // all hidden: unticked, and ticking shows them
    const metadata = screen.getByLabelText('Identifiers and metadata')
    expect(metadata).not.toBeChecked()
    fireEvent.click(metadata)
    expect(props.onSetEdgeTypesHidden).toHaveBeenCalledWith(['rdf:type'], false)
  })

  it('hides a category\'s edge types when they are all shown and its box is unticked', () => {
    const props = renderControls({ edgeTypeGroups: [group('A', ['a:1', 'a:2']), group('B', ['b:1'])], onSetEdgeTypesHidden: vi.fn() })
    fireEvent.click(screen.getByLabelText('A'))
    expect(props.onSetEdgeTypesHidden).toHaveBeenCalledWith(['a:1', 'a:2'], true)
  })

  it('does not head the list with a category when there is only one', () => {
    renderControls({ edgeTypeGroups: [group('Other', ['x', 'y'])] })
    expect(screen.queryByLabelText('Other')).toBeNull()
  })
})
