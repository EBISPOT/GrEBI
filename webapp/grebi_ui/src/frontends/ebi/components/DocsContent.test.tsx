import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, act } from '@testing-library/react'
import DocsContent from './DocsContent'

// the API reference asks the API for its description; here it never answers
vi.mock('../../../app/api', async (importOriginal) => {
  const mod: any = await importOriginal()
  return { ...mod, get: vi.fn(() => new Promise(() => {})), getPaginated: vi.fn(), post: vi.fn() }
})
import { resetPubmedRefs } from './pubmedRegistry'

// the parameter labels have no htmlFor, so find the input that follows the label
const paramInput = (name: string) =>
  screen.getByText(name, { selector: 'label' }).nextElementSibling as HTMLInputElement

// the interactive examples embedded in the docs call fetch on mount
beforeEach(() => {
  resetPubmedRefs()
  vi.stubGlobal('fetch', vi.fn(async () => ({ ok: true, json: async () => ['g1'] })))
})
afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
})

const renderDocs = (markdown: string, props: Partial<React.ComponentProps<typeof DocsContent>> = {}) =>
  render(<DocsContent markdown={markdown} images={{}} {...props} />)

describe('DocsContent', () => {
  it('gives headings slug ids and section numbers down to h3, ignoring headings inside code fences', () => {
    const markdown = [
      '# Getting Started', '', '## Install It', '', '### Step One', '',
      '```bash', '# not a heading', '```', '', '#### Deep heading',
    ].join('\n')
    renderDocs(markdown, { sectionNumber: 3 })

    const h1 = screen.getByRole('heading', { level: 1 })
    expect(h1).toHaveAttribute('id', 'getting-started')
    expect(h1).toHaveTextContent('3Getting Started')
    expect(screen.getByRole('heading', { level: 2 })).toHaveAttribute('id', 'install-it')
    expect(screen.getByRole('heading', { level: 2 })).toHaveTextContent('3.1Install It')
    expect(screen.getByRole('heading', { level: 3 })).toHaveTextContent('3.1.1Step One')
    // h4 gets an id but no number
    expect(screen.getByRole('heading', { level: 4 })).toHaveAttribute('id', 'deep-heading')
    expect(screen.getByRole('heading', { level: 4 })).toHaveTextContent(/^Deep heading$/)
    expect(screen.getAllByRole('heading')).toHaveLength(4)
    expect(document.querySelector('code.language-bash')).toHaveTextContent('# not a heading')
  })

  it('leaves headings unnumbered without a section number', () => {
    renderDocs('# Title\n\n## Sub')
    expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(/^Title$/)
    expect(screen.getByRole('heading', { level: 2 })).toHaveTextContent(/^Sub$/)
  })

  it('inlines images from the manifest and leaves unknown image paths alone', () => {
    renderDocs('![Logo](./images/logo.png)\n\n![Missing](./images/nope.png)', {
      images: { 'logo.png': 'data:image/png;base64,AAAA' },
    })
    expect(screen.getByAltText('Logo')).toHaveAttribute('src', 'data:image/png;base64,AAAA')
    expect(screen.getByAltText('Missing')).toHaveAttribute('src', './images/nope.png')
  })

  it('opens external links in a new tab but keeps in-page anchors in place', () => {
    renderDocs('See [EBI](https://www.ebi.ac.uk) or [below](#below).')
    const ebi = screen.getByRole('link', { name: 'EBI' })
    expect(ebi).toHaveAttribute('target', '_blank')
    expect(ebi).toHaveAttribute('rel', 'noopener noreferrer')
    const below = screen.getByRole('link', { name: 'below' })
    expect(below).toHaveAttribute('href', '#below')
    expect(below).not.toHaveAttribute('target')
  })

  it('renders the custom todo, api-example and query-template elements', async () => {
    renderDocs([
      '<todo>Write this section</todo>', '',
      '<api-example method="POST" url="/api/v1/graphs/g1/search" q="BRCA1" size="5"></api-example>', '',
      '<query-template id="gwas_by_trait" graph="g1" trait_id="mondo:1"></query-template>',
    ].join('\n'))

    expect(screen.getByText('TODO:').nextElementSibling).toHaveTextContent('Write this section')
    expect(screen.getByText('POST')).toBeInTheDocument()
    expect(screen.getByText('/api/v1/graphs/g1/search')).toBeInTheDocument()
    expect(paramInput('q')).toHaveValue('BRCA1')
    // `size` reaches ApiExample as a number (a known HTML attribute) and is still a parameter
    expect(paramInput('size')).toHaveValue('5')
    expect(screen.getByText('gwas_by_trait')).toBeInTheDocument()
    expect(screen.getByText('graph: g1')).toBeInTheDocument()
    expect(paramInput('trait_id')).toHaveValue('mondo:1')
    await act(async () => {})
  })

  it('renders the openapi-reference element, which loads the description from the API', () => {
    renderDocs('# API reference\n\n<openapi-reference></openapi-reference>')
    expect(screen.getByText('Loading the API description…')).toBeInTheDocument()
  })

  it('numbers pubmed citations and lists the references straight away', async () => {
    const markdown = 'As shown<pubmed id="40323307"></pubmed> before.'
    renderDocs(markdown)
    await act(async () => {})
    const cite = screen.getByRole('link', { name: '[1]' })
    expect(cite).toHaveAttribute('href', '#ref-1')

    expect(screen.getByText(/OLS4: a new Ontology Lookup Service/)).toBeInTheDocument()
    expect(screen.getByRole('link', { name: /McLaughlin J/ })).toHaveAttribute('href', 'https://pubmed.ncbi.nlm.nih.gov/40323307/')
  })

  it('adds a copy button to code blocks that confirms briefly', async () => {
    const writeText = vi.fn(async () => {})
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    renderDocs('```python\nprint(1)\n```')
    vi.useFakeTimers()

    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: 'Copy' }))
    })
    expect(writeText).toHaveBeenCalledTimes(1)
    expect(writeText).toHaveBeenCalledWith(expect.stringMatching(/^print\(1\)\n?$/))
    expect(screen.getByRole('button', { name: 'Copied!' })).toBeInTheDocument()
    act(() => { vi.advanceTimersByTime(1500) })
    expect(screen.getByRole('button', { name: 'Copy' })).toBeInTheDocument()
    expect(document.querySelector('code.language-python')).toHaveTextContent('print(1)')
  })

  it('lists "See also" pages that navigate through the callback instead of the link', () => {
    const onNavigate = vi.fn()
    renderDocs('Body', { seeAlso: [{ title: 'Other page', anchor: 'other' }], onNavigate })
    const link = screen.getByRole('link', { name: 'Other page' })
    expect(link).toHaveAttribute('href', '#other')
    fireEvent.click(link)
    expect(onNavigate).toHaveBeenCalledWith('other')
    expect(screen.getByRole('heading', { name: 'See also' })).toBeInTheDocument()
  })

  it('renders GFM tables and inline code', () => {
    renderDocs('| a | b |\n|---|---|\n| 1 | `x` |')
    expect(screen.getByRole('columnheader', { name: 'a' })).toBeInTheDocument()
    expect(screen.getByRole('cell', { name: '1' })).toBeInTheDocument()
    expect(screen.getByRole('cell', { name: 'x' }).querySelector('code')).not.toBeNull()
  })
})
