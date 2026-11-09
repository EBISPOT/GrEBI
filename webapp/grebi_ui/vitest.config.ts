import { defineConfig } from 'vitest/config'
import { createRequire } from 'module'
import yaml from 'js-yaml'

const require = createRequire(import.meta.url)

// Unit tests for the UI: components render in jsdom against mocked API calls.
// Run with `npm test`; CI runs them in .github/workflows/ui_tests.yml.
export default defineConfig({
  esbuild: { jsx: 'automatic' },
  plugins: [
    {
      // YAML files (such as configs/ui_configs/graph_view.yaml) are imported as
      // the data they hold, as build.mjs does for the bundle.
      name: 'yaml',
      enforce: 'pre',
      transform(code: string, id: string) {
        if (!/\.ya?ml$/.test(id)) return null
        return { code: `export default ${JSON.stringify(yaml.load(code))};`, map: null }
      },
    },
  ],
  resolve: {
    // MUI's CommonJS build (the one Node loads here) requires emotion's
    // CommonJS build; the sources must get the same copy, or a CacheProvider
    // (the widgets') is a different React context from the one MUI reads.
    alias: {
      '@emotion/react': require.resolve('@emotion/react'),
      '@emotion/cache': require.resolve('@emotion/cache'),
    },
  },
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['src/test/setup.ts'],
    include: ['src/**/*.test.{ts,tsx}'],
    css: false,
    // headroom for slow CI runners; individual waits are bounded by Testing
    // Library's asyncUtilTimeout in src/test/setup.ts
    testTimeout: 20000,
    hookTimeout: 20000,
    coverage: {
      provider: 'v8',
      include: ['src/**/*.{ts,tsx}'],
      exclude: ['src/**/*.test.{ts,tsx}', 'src/test/**', 'src/index_ebi.tsx', 'src/reportWebVitals.ts', 'src/components/node_prop_table/testNode.ts'],
      // measured at 89.7% statements / 89.4% branches / 85.8% functions when
      // the suite was written (2026-09); fail CI well below that, not at it
      thresholds: { statements: 80, branches: 80, functions: 75, lines: 80 },
    },
  },
})
