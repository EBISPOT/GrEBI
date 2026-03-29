import { useState, useCallback } from "react";
import { ChevronRight, ExpandMore } from "@mui/icons-material";
import type { SidebarEntry } from "../pages/DocsPage";

function slugFromPath(path: string): string {
  return path.replace(/\.md$/, "");
}

function SidebarItem({
  entry,
  depth,
  activeSlug,
  onNavigate,
}: {
  entry: SidebarEntry;
  depth: number;
  activeSlug: string;
  onNavigate: (slug: string) => void;
}) {
  const slug = entry.path ? slugFromPath(entry.path) : undefined;
  const isActive = slug === activeSlug;

  // Auto-expand if this branch contains the active page
  const branchContainsActive = useCallback(
    (e: SidebarEntry): boolean => {
      if (e.path && slugFromPath(e.path) === activeSlug) return true;
      return (e.children || []).some(branchContainsActive);
    },
    [activeSlug]
  );

  const [expanded, setExpanded] = useState(() => branchContainsActive(entry));

  const hasChildren = entry.children && entry.children.length > 0;

  return (
    <li>
      <div
        className={`flex items-center cursor-pointer select-none px-2 py-1 rounded text-sm ${
          isActive
            ? "bg-blue-100 text-blue-800 font-semibold"
            : "hover:bg-gray-100 text-gray-700"
        }`}
        style={{ paddingLeft: `${depth * 16 + 8}px` }}
        onClick={() => {
          if (slug) {
            onNavigate(slug);
            if (hasChildren) setExpanded(true);
          } else if (hasChildren) {
            setExpanded((prev) => !prev);
          }
        }}
      >
        {hasChildren && (
          <span
            className="mr-1 text-gray-400"
            onClick={(e) => {
              e.stopPropagation();
              setExpanded((prev) => !prev);
            }}
          >
            {expanded ? (
              <ExpandMore fontSize="small" />
            ) : (
              <ChevronRight fontSize="small" />
            )}
          </span>
        )}
        {!hasChildren && <span className="mr-1 w-5 inline-block" />}
        {entry.title}
      </div>
      {hasChildren && expanded && (
        <ul>
          {entry.children!.map((child, i) => (
            <SidebarItem
              key={child.path || i}
              entry={child}
              depth={depth + 1}
              activeSlug={activeSlug}
              onNavigate={onNavigate}
            />
          ))}
        </ul>
      )}
    </li>
  );
}

export default function DocsSidebar({
  sidebar,
  activeSlug,
  onNavigate,
}: {
  sidebar: SidebarEntry[];
  activeSlug: string;
  onNavigate: (slug: string) => void;
}) {
  return (
    <nav
      className="w-64 flex-shrink-0 border-r border-gray-200 bg-gray-50 overflow-y-auto py-4"
      style={{ minHeight: "100%" }}
    >
      <ul className="space-y-0.5">
        {sidebar.map((entry, i) => (
          <SidebarItem
            key={entry.path || i}
            entry={entry}
            depth={0}
            activeSlug={activeSlug}
            onNavigate={onNavigate}
          />
        ))}
      </ul>
    </nav>
  );
}
