// YAML files are imported as the data they hold (build.mjs and vitest.config.ts
// turn them into JSON); what that data is, the importer checks.
declare module "*.yaml" {
  const data: unknown;
  export default data;
}
