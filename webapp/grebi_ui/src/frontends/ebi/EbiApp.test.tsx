import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'

// build.mjs bakes PUBLIC_URL into the bundle; the router and the logo link need it
process.env.PUBLIC_URL = '/'

// The app builds its own BrowserRouter, so routing is exercised through the
// real window location. Pages are stubbed: this test is about the route table.
vi.mock('./pages/EbiHomePage', () => ({ default: () => <div data-testid="page">HomePage</div> }))
vi.mock('./pages/EbiDatasourcesPage', () => ({ default: () => <div data-testid="page">DatasourcesPage</div> }))
vi.mock('./pages/EbiGraphPage', () => ({ default: () => <div data-testid="page">GraphPage</div> }))
vi.mock('./pages/EbiSearchPage', () => ({ default: () => <div data-testid="page">SearchPage</div> }))
vi.mock('./pages/EbiEdgeSearchPage', () => ({ default: () => <div data-testid="page">EdgeSearchPage</div> }))
vi.mock('./pages/EbiLookupPage', () => ({ default: () => <div data-testid="page">LookupPage</div> }))
vi.mock('./pages/EbiNodePage', () => ({ default: () => <div data-testid="page">NodePage</div> }))
vi.mock('./pages/EbiTablesHomePage', () => ({ default: () => <div data-testid="page">TablesHomePage</div> }))
vi.mock('./pages/EbiTablesPage', () => ({ default: () => <div data-testid="page">TablesPage</div> }))
vi.mock('./pages/EbiQueriesHomePage', () => ({ default: () => <div data-testid="page">QueriesHomePage</div> }))
vi.mock('./pages/EbiQueryOrTopicPage', () => ({ default: () => <div data-testid="page">QueryOrTopicPage</div> }))
vi.mock('./pages/EbiDownloadsPage', () => ({ default: () => <div data-testid="page">DownloadsPage</div> }))
vi.mock('./pages/DocsPage', () => ({ default: () => <div data-testid="page">DocsPage</div> }))
vi.mock('./pages/EbiErrorPage', () => ({ default: () => <div data-testid="page">ErrorPage</div> }))

// the layout's nav asks the API for the graph list when none is remembered
vi.mock('../../app/api', () => ({ get: vi.fn(async () => ['g1']), getPaginated: vi.fn(), post: vi.fn() }))

import EbiApp from './EbiApp'

beforeEach(() => {
  sessionStorage.clear()
})

afterEach(() => {
  window.history.replaceState(null, '', '/')
  process.env.PUBLIC_URL = '/'
})

function renderAt(path: string) {
  window.history.replaceState(null, '', path)
  return render(<EbiApp />)
}

describe('EbiApp routes', () => {
  it.each([
    ['/', 'HomePage'],
    ['/graphs', 'DatasourcesPage'],
    ['/graphs/g1', 'GraphPage'],
    ['/graphs/g1/search', 'SearchPage'],
    ['/graphs/g1/edges', 'EdgeSearchPage'],
    ['/graphs/g1/lookup', 'LookupPage'],
    ['/graphs/g1/nodes/bW9uZG86MQ', 'NodePage'],
    ['/tables', 'TablesHomePage'],
    ['/graphs/g1/tables', 'TablesHomePage'],
    ['/graphs/g1/tables/mq1', 'TablesPage'],
    ['/graphs/g1/queries', 'QueriesHomePage'],
    ['/graphs/g1/queries/gwas', 'QueryOrTopicPage'],
    ['/graphs/g1/downloads', 'DownloadsPage'],
    ['/docs', 'DocsPage'],
    ['/docs/queries/nested', 'DocsPage'],
    ['/error', 'ErrorPage'],
    ['/no/such/page', 'ErrorPage'],
  ])('%s renders %s', async (path, expected) => {
    renderAt(path)
    expect(await screen.findByTestId('page')).toHaveTextContent(expected)
  })

  it('wraps every page in the layout with the navigation menu', async () => {
    renderAt('/graphs/g1/queries')
    await screen.findByTestId('page')
    expect(screen.getByRole('menuitem', { name: 'Queries' })).toBeInTheDocument()
    expect(screen.getByRole('menuitem', { name: 'Docs' })).toBeInTheDocument()
    expect(screen.getByAltText('GrEBI logo')).toBeInTheDocument()
  })

  it('honours the deployment prefix from PUBLIC_URL as the router basename', async () => {
    process.env.PUBLIC_URL = '/kg/'
    renderAt('/kg/graphs')
    expect(await screen.findByTestId('page')).toHaveTextContent('DatasourcesPage')
  })
})
