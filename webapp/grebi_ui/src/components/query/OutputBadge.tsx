import { Output } from "@mui/icons-material";

export default function OutputBadge({ children, size = "sm" }: { children: React.ReactNode; size?: "xs" | "sm" }) {
  const textSize = size === "xs" ? "text-xs" : "text-sm";
  const iconSize = size === "xs" ? 14 : 16;
  return (
    <code className={`font-mono font-bold ${textSize} bg-green-50 text-green-700 rounded px-1.5 py-0.5 inline-flex items-center gap-0.5`}>
      <Output sx={{ fontSize: iconSize }} />{children}
    </code>
  );
}
