import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import WidgetExample from './WidgetExample'

describe('WidgetExample', () => {
  it('frames a widget and names its element', () => {
    render(
      <WidgetExample>
        {/* @ts-ignore a custom element, inert in jsdom */}
        <grebi-graph graph="g" node="n" data-testid="widget" />
      </WidgetExample>
    )
    expect(screen.getByText('Live widget')).toBeInTheDocument()
    expect(screen.getByText('<grebi-graph>')).toBeInTheDocument()
    expect(screen.getByText(/everything inside the frame is the widget/)).toBeInTheDocument()
    expect(screen.getByTestId('widget').closest('figure')).not.toBeNull()
  })

  it('needs no element to name', () => {
    render(<WidgetExample><div>plain</div></WidgetExample>)
    expect(screen.getByText('Live widget')).toBeInTheDocument()
    expect(screen.queryByText(/^<grebi-/)).toBeNull()
  })
})
