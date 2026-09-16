import '@testing-library/jest-dom/vitest'

// build.mjs bakes these into the bundle via esbuild `define`; tests read them
// from process.env directly.
process.env.REACT_APP_APIURL = 'http://localhost:3000/'
