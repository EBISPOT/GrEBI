import React from "react";
import { Helmet } from "react-helmet";
import { useEmbedded } from "../app/embedded";

/**
 * A page's entries for the document head (its title and description), except
 * inside a widget, which must leave the host page's head alone.
 */
export default function PageHead({ children }: { children?: React.ReactNode }) {
  const embedded = useEmbedded();
  if (embedded) {
    return null;
  }
  return <Helmet>{children}</Helmet>;
}
