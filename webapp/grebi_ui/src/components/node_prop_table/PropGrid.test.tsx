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
})
