import { defineConfig } from 'vitest/config'

// Unit tests for the UI: components render in jsdom against mocked API calls.
// Run with `npm test`; CI runs them in .github/workflows/ui_tests.yml.
export default defineConfig({
  esbuild: { jsx: 'automatic' },
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['src/test/setup.ts'],
    include: ['src/**/*.test.{ts,tsx}'],
    css: false,
  },
})
