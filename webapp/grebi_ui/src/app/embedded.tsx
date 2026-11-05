import { createContext, useContext } from "react";

/**
 * True inside a widget on another site (src/widgets). The pages then leave
 * out the site's chrome (breadcrumbs, the document title) and keep their
 * overlays to themselves; the query page shows results alone when it can.
 */
export const EmbeddedContext = createContext(false);

export function useEmbedded(): boolean {
  return useContext(EmbeddedContext);
}
