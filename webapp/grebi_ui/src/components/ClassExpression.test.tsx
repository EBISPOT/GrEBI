import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import ClassExpression from './ClassExpression'
import Refs from '../model/Refs'
import encodeNodeId from '../encodeNodeId'

// Expressions arrive as the JSON value of a node property, with the nodes
// they mention resolved through the node's _refs.
const refs = new Refs({
  'obo:BFO_0000050': { 'grebi:nodeId': 'bfo50', 'grebi:name': ['part of'] },
  'obo:UBERON_0000955': { 'grebi:nodeId': 'ub955', 'grebi:name': ['brain'] },
  'obo:UBERON_0000956': { 'grebi:nodeId': 'ub956', 'grebi:name': ['cerebral cortex'] },
  'obo:RO_0002202': { 'grebi:nodeId': 'ro2202', 'grebi:name': ['develops from'] },
  'xsd:integer': { 'grebi:nodeId': 'xsdint', 'grebi:name': ['integer'] },
})

const restriction = (props: Record<string, any>) => ({ 'rdf:type': ['owl:Restriction'], ...props })
const partOfCortex = restriction({ 'owl:onProperty': 'obo:BFO_0000050', 'owl:someValuesFrom': 'obo:UBERON_0000956' })

function renderExpr(expr: any) {
  const utils = render(
    <MemoryRouter>
      <ClassExpression graph="g" refs={refs} expr={expr} />
    </MemoryRouter>
  )
  return {
    ...utils,
    text: () => utils.container.textContent,
    links: () => screen.getAllByRole('link').map((l) => l.textContent),
  }
}

let consoleError: ReturnType<typeof vi.spyOn>
beforeEach(() => {
  // the component builds its child arrays without keys; keep that noise out of the run
  const original = console.error
  consoleError = vi.spyOn(console, 'error').mockImplementation((...args: any[]) => {
    if (typeof args[0] === 'string' && args[0].includes('unique "key"')) return
    original(...args)
  })
})
afterEach(() => consoleError.mockRestore())

describe('ClassExpression', () => {
  it('renders a known class as a link to its node and an unknown IRI as an OLS search link', () => {
    const { unmount } = renderExpr('obo:UBERON_0000955')
    expect(screen.getByRole('link', { name: 'brain' })).toHaveAttribute(
      'href',
      `/graphs/g/nodes/${encodeNodeId('obo:UBERON_0000955')}`
    )
    unmount()

    renderExpr('zzz:9999999')
    const link = screen.getByRole('link')
    expect(link).toHaveAttribute('href', 'https://www.ebi.ac.uk/ols4/search?q=zzz%3A9999999')
    expect(link).toHaveTextContent('zzz:9999999')
  })

  it('links an id of a known kind that is not in the refs to its database', () => {
    renderExpr('obo:UBERON_0002048')
    const link = screen.getByRole('link', { name: 'obo:UBERON_0002048' })
    expect(link).toHaveAttribute('href', 'https://www.ebi.ac.uk/ols4/ontologies/uberon/classes?iri=http://purl.obolibrary.org/obo/UBERON_0002048')
    expect(link).toHaveAttribute('target', '_blank')
  })

  it('renders an intersection with a nested restriction in parentheses', () => {
    const { text, links } = renderExpr({
      'rdf:type': ['owl:Class'],
      'owl:intersectionOf': ['obo:UBERON_0000955', partOfCortex],
    })
    expect(links()).toEqual(['brain', 'part of', 'cerebral cortex'])
    expect(screen.getByText('and')).toBeInTheDocument()
    expect(screen.getByText('some')).toBeInTheDocument()
    expect(text()).toBe('(brainandpart ofsomecerebral cortex)')
  })

  it('renders unions with "or" and complements with "not"', () => {
    const union = renderExpr({ 'rdf:type': ['owl:Class'], 'owl:unionOf': ['obo:UBERON_0000955', 'obo:UBERON_0000956'] })
    expect(union.text()).toBe('(brainorcerebral cortex)')
    union.unmount()

    const complement = renderExpr({ 'rdf:type': ['owl:Class'], 'owl:complementOf': 'obo:UBERON_0000955' })
    expect(complement.text()).toBe('notbrain')
  })

  it('renders enumerations in braces', () => {
    const { text } = renderExpr({ 'rdf:type': ['owl:Class'], 'owl:oneOf': ['obo:UBERON_0000955', 'obo:UBERON_0000956'] })
    expect(text()).toBe('{brain,\u00a0cerebral cortex}')
  })

  it('renders existential, universal, value and self restrictions on a property', () => {
    const cases: [any, string][] = [
      [partOfCortex, 'part ofsomecerebral cortex'],
      [restriction({ 'owl:onProperty': 'obo:BFO_0000050', 'owl:allValuesFrom': 'obo:UBERON_0000956' }), 'part ofonlycerebral cortex'],
      [restriction({ 'owl:onProperty': 'obo:RO_0002202', 'owl:hasValue': 'obo:UBERON_0000955' }), 'develops fromvaluebrain'],
      [restriction({ 'owl:onProperty': 'obo:BFO_0000050', 'owl:hasSelf': true }), 'part ofSelf'],
    ]
    for (const [expr, expected] of cases) {
      const { text, unmount } = renderExpr(expr)
      expect(text()).toBe(expected)
      unmount()
    }
  })

  it('renders cardinality restrictions with their number', () => {
    const min = renderExpr(restriction({ 'owl:onProperty': 'obo:BFO_0000050', 'owl:minCardinality': 2 }))
    expect(min.text()).toBe('part ofmin2')
    min.unmount()

    const qualified = renderExpr(
      restriction({ 'owl:onProperty': 'obo:BFO_0000050', 'owl:onClass': 'obo:UBERON_0000956', 'owl:qualifiedCardinality': '3' })
    )
    expect(qualified.text()).toBe('part ofexactly3\u00a0cerebral cortex')
  })

  it('renders datatype restrictions and datatype definitions', () => {
    const ranged = renderExpr({
      'rdf:type': ['rdfs:Datatype'],
      'owl:onDatatype': 'xsd:integer',
      'owl:withRestrictions': [
        { 'http://www.w3.org/2001/XMLSchema#minInclusive': 5 },
        { 'http://www.w3.org/2001/XMLSchema#maxExclusive': 10 },
      ],
    })
    expect(ranged.text()).toBe('integer[≥ 5, < 10]')
    ranged.unmount()

    const defined = renderExpr({ type: 'datatype', label: 'small number', 'owl:equivalentClass': 'xsd:integer' })
    expect(defined.text()).toBe('small number integer')
  })

  it('renders inverse properties and flags expressions it does not understand', () => {
    const inverse = renderExpr({ 'owl:inverseOf': 'obo:BFO_0000050' })
    expect(inverse.text()).toBe('inverse(part of)')
    inverse.unmount()

    const unknown = renderExpr({ 'rdf:type': ['owl:Restriction'], 'owl:onProperties': ['obo:BFO_0000050'] })
    expect(unknown.text()).toBe(
      'unknown class expression {"rdf:type":["owl:Restriction"],"owl:onProperties":["obo:BFO_0000050"]}'
    )
  })
})
