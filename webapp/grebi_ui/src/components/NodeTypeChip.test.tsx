import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import NodeTypeChip from './NodeTypeChip'

describe('NodeTypeChip', () => {
  it('shows the short name of the type in small caps', () => {
    render(<NodeTypeChip type={{ longName: 'Ontology Class', shortName: 'Class' }} />)
    const chip = screen.getByText('Class')
    expect(chip.tagName).toBe('SPAN')
    expect(chip).toHaveStyle({ textTransform: 'uppercase', fontWeight: 'bold' })
    expect(screen.queryByText('Ontology Class')).toBeNull()
  })
})
