import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, act } from '@testing-library/react'
import DocsSidebar from './DocsSidebar'

const sidebar = [
  {
    title: 'Intro', anchor: 'intro',
    children: [
      { title: 'Getting started', anchor: 'getting-started' },
      { title: 'The `grebi` CLI', anchor: 'the-grebi-cli' },
    ],
  },
  { title: 'Queries', anchor: 'queries', children: [{ title: 'Templates', anchor: 'templates' }] },
  { title: 'Unlinked group', children: [{ title: 'Leaf', anchor: 'leaf' }] },
]

function renderSidebar(activeAnchor = '') {
  const onNavigate = vi.fn()
  render(<DocsSidebar sidebar={sidebar} activeAnchor={activeAnchor} onNavigate={onNavigate} />)
  return onNavigate
}

const row = (title: string) => screen.getByText(title).closest('div')!

describe('DocsSidebar', () => {
  it('numbers entries, expands only the branch holding the active anchor, and renders backticks as code', () => {
    renderSidebar('getting-started')
    const nav = screen.getByRole('navigation')
    expect(nav).toHaveTextContent('1Intro')
    expect(nav).toHaveTextContent('1.1Getting started')
    expect(nav).toHaveTextContent('1.2The grebi CLI')
    expect(nav).toHaveTextContent('2Queries')
    expect(nav).toHaveTextContent('3Unlinked group')
    expect(screen.getByText('grebi').tagName).toBe('CODE')
    // collapsed branches
    expect(screen.queryByText('Templates')).toBeNull()
    expect(screen.queryByText('Leaf')).toBeNull()
  })

  it('highlights the active entry', () => {
    renderSidebar('queries')
    expect(row('Queries').className).toContain('bg-blue-100')
    expect(row('Intro').className).not.toContain('bg-blue-100')
  })

  it('clicking an entry navigates to its anchor and expands its children', () => {
    const onNavigate = renderSidebar()
    fireEvent.click(screen.getByText('Queries'))
    expect(onNavigate).toHaveBeenCalledWith('queries')
    expect(screen.getByText('Templates')).toBeInTheDocument()
    fireEvent.click(screen.getByText('Templates'))
    expect(onNavigate).toHaveBeenLastCalledWith('templates')
  })

  it('the chevron toggles a branch without navigating', () => {
    const onNavigate = renderSidebar()
    const chevron = row('Queries').querySelector('[data-testid="ChevronRightIcon"]')!
    fireEvent.click(chevron)
    expect(screen.getByText('Templates')).toBeInTheDocument()
    expect(onNavigate).not.toHaveBeenCalled()
    fireEvent.click(row('Queries').querySelector('[data-testid="ExpandMoreIcon"]')!)
    expect(screen.queryByText('Templates')).toBeNull()
  })

  it('an entry without an anchor just toggles its children', () => {
    const onNavigate = renderSidebar()
    fireEvent.click(screen.getByText('Unlinked group'))
    expect(screen.getByText('Leaf')).toBeInTheDocument()
    expect(onNavigate).not.toHaveBeenCalled()
    fireEvent.click(screen.getByText('Unlinked group'))
    expect(screen.queryByText('Leaf')).toBeNull()
  })

  it('can be resized by dragging the handle, within limits', () => {
    const { container } = render(<DocsSidebar sidebar={sidebar} activeAnchor="" onNavigate={vi.fn()} />)
    const panel = container.firstElementChild as HTMLElement
    expect(panel.style.width).toBe('320px')

    const move = (movementX: number) => {
      const ev = new MouseEvent('mousemove')
      Object.defineProperty(ev, 'movementX', { value: movementX })
      act(() => { document.dispatchEvent(ev) })
    }
    fireEvent.mouseDown(container.querySelector('.cursor-col-resize')!)
    expect(document.body.style.cursor).toBe('col-resize')
    move(100)
    expect(panel.style.width).toBe('420px')
    move(1000)
    expect(panel.style.width).toBe('600px')
    move(-2000)
    expect(panel.style.width).toBe('220px')

    act(() => { document.dispatchEvent(new MouseEvent('mouseup')) })
    expect(document.body.style.cursor).toBe('')
    move(100)
    expect(panel.style.width).toBe('220px')
  })
})
