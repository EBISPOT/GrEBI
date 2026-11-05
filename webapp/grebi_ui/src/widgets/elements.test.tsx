import { describe, it, expect, vi, beforeAll, afterEach } from 'vitest'
import { waitFor } from '@testing-library/dom'
import { widgetPath, defineGrebiWidgets, loadFonts, KINDS } from './elements'
import { configureInstance } from '../app/instance'

vi.mock('./GrebiWidget', () => ({
  default: ({ path, shadowRoot, container }: any) => (
    <div data-testid="widget" data-in-shadow={String(container.getRootNode() === shadowRoot)}>{path}</div>
  ),
}))

function element(tag: string, attrs: Record<string, string>): Element {
  const el = document.createElement(tag)
  for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v)
  return el
}

describe('widgetPath', () => {
  it.each([
    ['grebi-node', { graph: 'dismech', node: 'hgnc:1100' }, '/graphs/dismech/nodes/aGduYzoxMTAw'],
    ['grebi-node', { graph: 'dismech', node: 'hgnc:1100', tab: 'properties', lang: 'fr' }, '/graphs/dismech/nodes/aGduYzoxMTAw?tab=properties&lang=fr'],
    ['grebi-graph', { graph: 'dismech', node: 'hgnc:1100' }, '/graphs/dismech/nodes/aGduYzoxMTAw?tab=graph'],
    ['grebi-graph', { graph: 'dismech', node: 'hgnc:1100', g: 'abc' }, '/graphs/dismech/nodes/aGduYzoxMTAw?tab=graph&g=abc'],
    ['grebi-edge', { graph: 'dismech', edge: 'e1' }, '/graphs/dismech/edges/ZTE'],
    ['grebi-query', { graph: 'dismech', query: 'disease_to_treatments', disease_id: 'mondo:0005002' }, '/graphs/dismech/queries/disease_to_treatments?disease_id=mondo%3A0005002'],
    ['grebi-query', { graph: 'dismech', query: 'disease_to_treatments', disease_id: 'mondo:0005002', inputs: 'true' }, '/graphs/dismech/queries/disease_to_treatments?disease_id=mondo%3A0005002&inputs=true'],
    ['grebi-queries', { graph: 'dismech' }, '/graphs/dismech/queries'],
    ['grebi-queries', { graph: 'dismech', topic: 'gwas' }, '/graphs/dismech/queries/gwas'],
    ['grebi-search', { graph: 'dismech', q: 'BRCA1' }, '/graphs/dismech/search?q=BRCA1'],
    ['grebi-lookup', { graph: 'dismech' }, '/graphs/dismech/lookup'],
    ['grebi-page', { path: '/graphs/dismech/tables' }, '/graphs/dismech/tables'],
    ['grebi-page', { path: '/graphs/dismech/search?q=BRCA1', size: '5' }, '/graphs/dismech/search?q=BRCA1&size=5'],
  ])('%s %o is %s', (tag, attrs, expected) => {
    expect(widgetPath(element(tag, attrs))).toBe(expected)
  })

  it('is null while the page is not named, and for other elements', () => {
    expect(widgetPath(element('grebi-node', { graph: 'dismech' }))).toBeNull()
    expect(widgetPath(element('grebi-node', { node: 'hgnc:1100' }))).toBeNull()
    expect(widgetPath(element('grebi-query', { graph: 'dismech' }))).toBeNull()
    expect(widgetPath(element('grebi-search', {}))).toBeNull()
    expect(widgetPath(element('grebi-page', { path: 'graphs/dismech' }))).toBeNull()
    expect(widgetPath(element('div', { graph: 'dismech', node: 'x' }))).toBeNull()
  })

  it('passes on every attribute but the HTML ones', () => {
    const el = element('grebi-node', {
      graph: 'g', node: 'n', class: 'card', style: 'margin: 1em', id: 'w', title: 'tip', hidden: '', role: 'region',
      tabindex: '0', 'data-x': '1', 'aria-label': 'node', tab: 'graph',
    })
    expect(widgetPath(el)).toBe('/graphs/g/nodes/bg?tab=graph')
  })

  it('escapes the graph and the query id in the path', () => {
    expect(widgetPath(element('grebi-query', { graph: 'a/b', query: 'c d' }))).toBe('/graphs/a%2Fb/queries/c%20d')
  })
})

describe('the widget elements', () => {
  beforeAll(() => {
    defineGrebiWidgets()
    // a second definition is a no-op
    defineGrebiWidgets()
    for (const tag of Object.keys(KINDS)) expect(customElements.get(tag)).toBeDefined()
  })

  afterEach(() => {
    document.body.innerHTML = ''
    configureInstance({ siteUrl: undefined })
  })

  const widgetIn = (el: Element) => el.shadowRoot!.querySelector('[data-testid="widget"]')

  it('renders the page in a shadow root with the site\'s stylesheet', async () => {
    configureInstance({ siteUrl: 'https://kg.example/spot/kg/' })
    document.body.innerHTML = '<grebi-node graph="dismech" node="hgnc:1100"></grebi-node>'
    const el = document.querySelector('grebi-node')!
    const sheet = el.shadowRoot!.querySelector('link[rel="stylesheet"]')!
    expect(sheet.getAttribute('href')).toBe('https://kg.example/spot/kg/widgets.css')
    await waitFor(() => expect(widgetIn(el)).toHaveTextContent('/graphs/dismech/nodes/aGduYzoxMTAw'))
    expect(widgetIn(el)!.getAttribute('data-in-shadow')).toBe('true')
    // nothing in the light DOM, nothing of the host's styles applies
    expect(el.childNodes.length).toBe(0)
  })

  it('shows another page when its attributes change', async () => {
    document.body.innerHTML = '<grebi-query graph="g" query="q" disease_id="mondo:1"></grebi-query>'
    const el = document.querySelector('grebi-query')!
    await waitFor(() => expect(widgetIn(el)).toHaveTextContent('?disease_id=mondo%3A1'))
    el.setAttribute('disease_id', 'mondo:2')
    await waitFor(() => expect(widgetIn(el)).toHaveTextContent('/graphs/g/queries/q?disease_id=mondo%3A2'))
  })

  it('shows nothing until the page is named', async () => {
    document.body.innerHTML = '<grebi-node graph="g"></grebi-node>'
    const el = document.querySelector('grebi-node')!
    await new Promise((r) => setTimeout(r, 20))
    expect(widgetIn(el)).toBeNull()
    el.setAttribute('node', 'n')
    await waitFor(() => expect(widgetIn(el)).toHaveTextContent('/graphs/g/nodes/bg'))
  })

  it('unmounts when removed from the page and renders again when put back', async () => {
    document.body.innerHTML = '<grebi-search graph="g"></grebi-search>'
    const el = document.querySelector('grebi-search')!
    await waitFor(() => expect(widgetIn(el)).toHaveTextContent('/graphs/g/search'))
    el.remove()
    await waitFor(() => expect(widgetIn(el)).toBeNull())
    document.body.appendChild(el)
    await waitFor(() => expect(widgetIn(el)).toHaveTextContent('/graphs/g/search'))
    // the shadow root was made once
    expect(el.shadowRoot!.querySelectorAll('link').length).toBe(1)
  })
})

describe('loadFonts', () => {
  it('adds the site\'s fonts to the document once', () => {
    const doc = document.implementation.createHTMLDocument()
    loadFonts(doc)
    loadFonts(doc)
    const links = Array.from(doc.head.querySelectorAll('link[data-grebi-fonts]'))
    expect(links.length).toBe(2)
    expect(links[0].getAttribute('href')).toContain('fonts.googleapis.com')
    expect(links[1].getAttribute('href')).toContain('EBI-Icon-fonts')
  })
})
