import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import DistributionPieChart from './DistributionPieChart'

// the legend under the chart is plain DOM; recharts itself needs a measured
// container, which jsdom does not provide, so the assertions target the legend
const legendEntries = () => screen.getAllByTitle(/.+/).map((el) => el.textContent)

describe('DistributionPieChart', () => {
  it('lists slices largest first with compact counts', () => {
    render(<DistributionPieChart title="Node Types" data={{ small: 12, big: 2_000_000, mid: 1500 }} />)
    expect(screen.getByText('Node Types')).toBeInTheDocument()
    expect(legendEntries()).toEqual(['big (2.0M)', 'mid (1.5K)', 'small (12)'])
  })

  it('folds everything beyond maxSlices into an "Other" slice with the summed count', () => {
    render(<DistributionPieChart title="t" maxSlices={2} data={{ a: 100, b: 50, c: 7, d: 3 }} />)
    expect(legendEntries()).toEqual(['a (100)', 'b (50)', 'Other (2) (10)'])
  })

  it('calls back with the slice name on click, except for the Other slice', () => {
    const onSliceClick = vi.fn()
    render(<DistributionPieChart title="t" maxSlices={1} data={{ a: 100, b: 50 }} onSliceClick={onSliceClick} />)
    fireEvent.click(screen.getByTitle('a'))
    expect(onSliceClick).toHaveBeenCalledWith('a')
    fireEvent.click(screen.getByTitle('Other (1)'))
    expect(onSliceClick).toHaveBeenCalledTimes(1)
    // only clickable entries are styled as links
    expect(screen.getByTitle('a').querySelector('.text-blue-600')).not.toBeNull()
    expect(screen.getByTitle('Other (1)').querySelector('.text-blue-600')).toBeNull()
  })

  it('renders entries as plain text when there is no click handler', () => {
    render(<DistributionPieChart title="t" data={{ a: 1 }} />)
    expect(screen.getByTitle('a').querySelector('.text-blue-600')).toBeNull()
  })

  it('shows a placeholder when there is no data', () => {
    render(<DistributionPieChart title="Edge Types" data={{}} />)
    expect(screen.getByText('Edge Types: no data')).toBeInTheDocument()
  })

  it('truncates long names in the legend but keeps the full name as a tooltip', () => {
    const long = 'a'.repeat(40)
    render(<DistributionPieChart title="t" data={{ [long]: 5 }} />)
    expect(screen.getByTitle(long)).toHaveTextContent(`${'a'.repeat(28)}… (5)`)
  })
})
