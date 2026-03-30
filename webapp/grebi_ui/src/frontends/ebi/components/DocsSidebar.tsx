import { useState, useCallback, useRef } from "react";
import { ChevronRight, ExpandMore } from "@mui/icons-material";
import type { SidebarEntry } from "../pages/DocsPage";

function slugFromPath(path: string): string {
  return path.replace(/\.md$/, "");
}

/** Render a title string, turning `backtick` segments into <code> spans. */
function renderTitle(title: string) {
  const parts = title.split(/(`[^`]+`)/);
  if (parts.length === 1) return title;
  return parts.map((part, i) => {
    if (part.startsWith("`") && part.endsWith("`")) {
      return <code key={i} className="font-mono bg-gray-100 rounded px-1">{part.slice(1, -1)}</code>;
    }
    return part;
  });
}

const INDENT = 20; // px per nesting level
const ICON_SIZE = 18;
const PL = 3; // pl-1 in px, tuned to align tree lines with icon centers
const GUTTER = PL + ICON_SIZE / 2; // center of expand icon = 12px
const TREE_COLOR = "#c9cdd1";

function SidebarItem({
  entry,
  depth,
  isLast,
  activeSlug,
  onNavigate,
}: {
  entry: SidebarEntry;
  depth: number;
  isLast: boolean;
  activeSlug: string;
  onNavigate: (slug: string) => void;
}) {
  const slug = entry.path ? slugFromPath(entry.path) : undefined;
  const isActive = slug === activeSlug;

  const branchContainsActive = useCallback(
    (e: SidebarEntry): boolean => {
      if (e.path && slugFromPath(e.path) === activeSlug) return true;
      return (e.children || []).some(branchContainsActive);
    },
    [activeSlug]
  );

  const [expanded, setExpanded] = useState(() => branchContainsActive(entry));

  const hasChildren = entry.children && entry.children.length > 0;

  const ROW_HEIGHT = 26;
  const CONNECTOR_Y = ROW_HEIGHT / 2;

  return (
    <li className="relative" style={{ paddingLeft: depth > 0 ? INDENT : 0 }}>
      {/* Vertical line from parent — extends full height unless last child */}
      {depth > 0 && (
        <span
          className="absolute top-0"
          style={{
            left: GUTTER,
            width: 1,
            background: TREE_COLOR,
            height: isLast ? CONNECTOR_Y : "100%",
          }}
        />
      )}
      {/* Horizontal connector from vertical line to item */}
      {depth > 0 && (
        <span
          className="absolute"
          style={{
            top: CONNECTOR_Y,
            left: GUTTER,
            width: INDENT - GUTTER,
            height: 1,
            background: TREE_COLOR,
          }}
        />
      )}
      <div
        className={`flex items-center cursor-pointer select-none pl-1 pr-2 rounded ${
          isActive
            ? "bg-blue-100 text-blue-800 font-semibold"
            : "hover:bg-gray-100 text-gray-700"
        }`}
        style={{ height: ROW_HEIGHT, fontSize: 16 }}
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
            className="mr-1 text-gray-400 flex-shrink-0 flex items-center justify-center"
            style={{ width: ICON_SIZE, height: ICON_SIZE }}
            onClick={(e) => {
              e.stopPropagation();
              setExpanded((prev) => !prev);
            }}
          >
            {expanded ? (
              <ExpandMore style={{ fontSize: ICON_SIZE }} />
            ) : (
              <ChevronRight style={{ fontSize: ICON_SIZE }} />
            )}
          </span>
        )}
        {!hasChildren && <span className="mr-1 inline-block flex-shrink-0" style={{ width: ICON_SIZE }} />}
        <span className="truncate">{entry.title ? renderTitle(entry.title) : null}</span>
      </div>
      {hasChildren && expanded && (
        <ul>
          {entry.children!.map((child, i) => (
            <SidebarItem
              key={child.path || i}
              entry={child}
              depth={depth + 1}
              isLast={i === entry.children!.length - 1}
              activeSlug={activeSlug}
              onNavigate={onNavigate}
            />
          ))}
        </ul>
      )}
    </li>
  );
}

const MIN_WIDTH = 220;
const MAX_WIDTH = 600;

export default function DocsSidebar({
  sidebar,
  activeSlug,
  onNavigate,
}: {
  sidebar: SidebarEntry[];
  activeSlug: string;
  onNavigate: (slug: string) => void;
}) {
  const [width, setWidth] = useState(320);
  const dragging = useRef(false);

  const onMouseDown = useCallback(() => {
    dragging.current = true;
    const onMouseMove = (e: MouseEvent) => {
      if (!dragging.current) return;
      setWidth((w) => {
        const next = w + e.movementX;
        return Math.min(MAX_WIDTH, Math.max(MIN_WIDTH, next));
      });
    };
    const onMouseUp = () => {
      dragging.current = false;
      document.removeEventListener("mousemove", onMouseMove);
      document.removeEventListener("mouseup", onMouseUp);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    };
    document.addEventListener("mousemove", onMouseMove);
    document.addEventListener("mouseup", onMouseUp);
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
  }, []);

  return (
    <div className="relative flex-shrink-0" style={{ width }}>
      <nav
        className="border-r border-gray-200 bg-gray-50 overflow-y-auto py-4 px-3 h-full"
      >
        <ul>
          {sidebar.map((entry, i) => (
            <SidebarItem
              key={entry.path || i}
              entry={entry}
              depth={0}
              isLast={i === sidebar.length - 1}
              activeSlug={activeSlug}
              onNavigate={onNavigate}
            />
          ))}
        </ul>
      </nav>
      {/* Drag handle */}
      <div
        className="absolute top-0 right-0 w-1 h-full cursor-col-resize hover:bg-blue-300 active:bg-blue-400 transition-colors"
        onMouseDown={onMouseDown}
      />
    </div>
  );
}
