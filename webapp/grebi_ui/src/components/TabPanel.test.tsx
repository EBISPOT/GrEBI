import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import TabPanel from './TabPanel'

describe('TabPanel', () => {
  it('shows its children when it is the selected tab', () => {
    render(<TabPanel value="props" index="props"><span>property table</span></TabPanel>)
    const panel = screen.getByRole('tabpanel')
    expect(panel).not.toHaveAttribute('hidden')
    expect(panel).toHaveAttribute('id', 'vertical-tabpanel-props')
    expect(panel).toHaveAttribute('aria-labelledby', 'vertical-tab-props')
    expect(screen.getByText('property table')).toBeInTheDocument()
  })

  it('is hidden and empty otherwise', () => {
    render(<TabPanel value="edges" index="props"><span>property table</span></TabPanel>)
    const panel = screen.getByRole('tabpanel', { hidden: true })
    expect(panel).toHaveAttribute('hidden')
    expect(panel).toBeEmptyDOMElement()
  })
})
