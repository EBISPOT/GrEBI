import { describe, it, expect, afterEach } from 'vitest'
import { apiUrl, siteUrl, configureInstance } from './instance'

afterEach(() => {
  configureInstance({ apiUrl: undefined, siteUrl: undefined })
})

describe('instance', () => {
  it('is the build\'s API and site unless configured', () => {
    expect(apiUrl()).toBe('http://localhost:3000/')
    expect(siteUrl()).toBe('/')
  })

  it('is what the widgets script configures, each on its own', () => {
    configureInstance({ siteUrl: 'https://kg.example/spot/kg/' })
    expect(siteUrl()).toBe('https://kg.example/spot/kg/')
    expect(apiUrl()).toBe('http://localhost:3000/')
    configureInstance({ apiUrl: 'https://kg.example/spot/kg/' })
    expect(apiUrl()).toBe('https://kg.example/spot/kg/')
    expect(siteUrl()).toBe('https://kg.example/spot/kg/')
  })
})
