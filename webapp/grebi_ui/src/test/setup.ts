import '@testing-library/jest-dom/vitest'
import { configure } from '@testing-library/dom'

// build.mjs bakes these into the bundle via esbuild `define`; tests read them
// from process.env directly. PUBLIC_URL feeds url-join, which throws on undefined.
process.env.REACT_APP_APIURL = 'http://localhost:3000/'
process.env.PUBLIC_URL = process.env.PUBLIC_URL || '/'

// The suite runs files in parallel workers; on a loaded CI runner a page can
// take longer than Testing Library's 1 s default to settle.
configure({ asyncUtilTimeout: 5000 })

// jsdom has no IntersectionObserver; pages that observe headings (the docs)
// must not blow up in any test, whichever order the files run in.
if (typeof (globalThis as any).IntersectionObserver === 'undefined') {
  ;(globalThis as any).IntersectionObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
    takeRecords() { return [] }
  }
}
