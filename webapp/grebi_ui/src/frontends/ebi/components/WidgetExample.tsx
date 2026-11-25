import React from "react";

/**
 * A live widget on a docs page (docs/widgets.md), in a frame that says so:
 * without one, a widget's own interface blends into the page around it and
 * the reader cannot tell the page from the example.
 */
export default function WidgetExample({ children }: { children?: React.ReactNode }) {
  const tag = React.Children.toArray(children)
    .map((child) => (React.isValidElement(child) && typeof child.type === "string" ? child.type : null))
    .find((type) => type && type.startsWith("grebi-"));
  return (
    <figure className="my-6 rounded-lg border-2 border-dashed border-purple-300 bg-purple-50 p-3">
      <figcaption className="flex flex-wrap items-baseline gap-x-2 gap-y-1 mb-2 text-xs text-purple-900">
        <span className="font-bold uppercase tracking-wide">Live widget</span>
        {tag && <code className="font-mono text-sm">&lt;{tag}&gt;</code>}
        <span className="text-purple-700">rendered on this page by that element: everything inside the frame is the widget, as your page would show it.</span>
      </figcaption>
      <div className="rounded-md overflow-hidden shadow bg-white">{children}</div>
    </figure>
  );
}
