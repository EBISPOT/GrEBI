import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Banner } from './Banner'

describe('Banner', () => {
  it('renders its children on a background that depends on the type', () => {
    const expected = { code: 'bg-neutral-light', info: 'bg-blue-50', warning: 'bg-yellow-200', error: 'bg-red-300' } as const
    for (const [type, cls] of Object.entries(expected)) {
      const { unmount } = render(<Banner type={type as any}>hello {type}</Banner>)
      expect(screen.getByText(`hello ${type}`)).toHaveClass(cls)
      unmount()
    }
  })

  it('shows an icon for info, warning and error', () => {
    const icons = { info: 'icon-info', warning: 'icon-exclamation-triangle', error: 'icon-exclamation-circle' } as const
    for (const [type, cls] of Object.entries(icons)) {
      const { container, unmount } = render(<Banner type={type as any}>x</Banner>)
      expect(container.querySelector(`i.${cls}`)).not.toBeNull()
      expect(container.querySelectorAll('i')).toHaveLength(1)
      unmount()
    }
  })

  it('renders code banners in monospace without an icon', () => {
    const { container } = render(<Banner type="code">curl ...</Banner>)
    expect(screen.getByText('curl ...')).toHaveClass('font-mono', 'whitespace-nowrap')
    expect(container.querySelector('i')).toBeNull()
  })
})
