/**
 * Which GrEBI this code talks to and links to. The app takes both from the
 * build (REACT_APP_APIURL and PUBLIC_URL); the widgets script (src/widgets)
 * sets them from where it was loaded, since it runs on other sites.
 */
let overrides: { apiUrl?: string; siteUrl?: string } = {};

export function configureInstance(config: { apiUrl?: string; siteUrl?: string }) {
  overrides = { ...overrides, ...config };
}

/** The API's base URL, ending in a slash: paths such as api/v1/graphs are appended to it. */
export function apiUrl(): string {
  return overrides.apiUrl ?? (process.env.REACT_APP_APIURL || "");
}

/**
 * The site's base URL, ending in a slash: a path on the app's own host, or an
 * absolute URL inside a widget on another site. Links to pages and to the
 * site's files (logo, icons) are joined onto it.
 */
export function siteUrl(): string {
  return overrides.siteUrl ?? (process.env.PUBLIC_URL || "/");
}
