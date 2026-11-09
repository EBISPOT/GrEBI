import React from "react";

/**
 * The layout of a property table: the labels down a column on the left, as
 * wide as the widest label up to 14rem, the values in the rest of the width,
 * and a thin rule between properties. A longer label wraps, at any character
 * if it must: a raw property key such as
 * biolink:chemical_to_disease_or_phenotypic_feature_association has no spaces,
 * and unbroken it made the column half the page wide. On a narrow screen each
 * value sits under its label.
 */
export function PropGrid({ children }: { children: React.ReactNode }) {
  return (
    <dl className="grid grid-cols-1 sm:grid-cols-[fit-content(14rem)_minmax(0,1fr)] sm:gap-x-6">
      {children}
    </dl>
  );
}

/** One property of a PropGrid: its label and whatever shows its values. */
export function PropGridRow({ label, children }: { label: React.ReactNode; children: React.ReactNode }) {
  return (
    <div className="contents">
      <dt className="pt-1.5 sm:py-1.5 sm:border-b border-neutral-light min-w-0 [overflow-wrap:anywhere]">{label}</dt>
      <dd className="pb-1.5 sm:pt-1.5 border-b border-neutral-light">{children}</dd>
    </div>
  );
}
