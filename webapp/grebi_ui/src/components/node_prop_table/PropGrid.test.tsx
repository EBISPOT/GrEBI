import { describe, it, expect } from 'vitest'
import { render } from '@testing-library/react'
import { PropGrid, PropGridRow } from './PropGrid'

describe('PropGrid', () => {
  it('is a description list of labels and values, one row per property', () => {
    const { container } = render(
      <PropGrid>
        <PropGridRow label="Name">psoriasis</PropGridRow>
        <PropGridRow label={<b>Type</b>}>disease</PropGridRow>
      </PropGrid>
    )
    const dl = container.querySelector('dl')!
    expect(dl.className).toContain('grid')
    expect(Array.from(dl.querySelectorAll('dt')).map((e) => e.textContent)).toEqual(['Name', 'Type'])
    expect(Array.from(dl.querySelectorAll('dd')).map((e) => e.textContent)).toEqual(['psoriasis', 'disease'])
    expect(dl.querySelector('dt + dd')).toHaveTextContent('psoriasis')
  })

  it('caps the label column and lets a label without spaces wrap inside it', () => {
    const { container } = render(<PropGrid><PropGridRow label="biolink:a_very_long_key_without_spaces">x</PropGridRow></PropGrid>)
    expect(container.querySelector('dl')!.className).toContain('fit-content(14rem)')
    // without this a label's longest word sets the column's width, whatever the cap
    expect(container.querySelector('dt')!.className).toContain('[overflow-wrap:anywhere]')
  })
})
