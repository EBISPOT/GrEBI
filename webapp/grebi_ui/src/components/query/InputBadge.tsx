import { Input } from "@mui/icons-material";

export default function InputBadge({ children, size = "sm" }: { children: React.ReactNode; size?: "xs" | "sm" }) {
  const textSize = size === "xs" ? "text-xs" : "text-sm";
  const iconSize = size === "xs" ? 14 : 16;
  return (
    <code className={`font-mono font-bold ${textSize} bg-blue-50 text-blue-700 rounded px-1.5 py-0.5 inline-flex items-center gap-0.5`}>
      <Input sx={{ fontSize: iconSize }} />{children}
    </code>
  );
}
