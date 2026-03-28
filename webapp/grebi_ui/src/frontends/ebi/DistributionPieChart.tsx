import React, { useMemo } from "react";
import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer } from "recharts";

const COLORS = [
  "#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f",
  "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ac",
  "#86bcb6", "#8cd17d",
];

function formatCount(n: number): string {
  if (n >= 1e9) return (n / 1e9).toFixed(1) + "B";
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "K";
  return n.toLocaleString();
}

interface Props {
  data: Record<string, number>;
  title: string;
  maxSlices?: number;
  onSliceClick?: (name: string) => void;
}

export default function DistributionPieChart({ data, title, maxSlices = 10, onSliceClick }: Props) {
  const slices = useMemo(() => {
    const entries = Object.entries(data).sort((a, b) => b[1] - a[1]);
    const top = entries.slice(0, maxSlices);
    const rest = entries.slice(maxSlices);
    const result = top.map(([name, value]) => ({ name, value }));
    if (rest.length > 0) {
      result.push({
        name: `Other (${rest.length})`,
        value: rest.reduce((sum, [, v]) => sum + v, 0),
      });
    }
    return result;
  }, [data, maxSlices]);

  if (slices.length === 0) {
    return (
      <div className="text-center text-gray-400 text-sm py-8">{title}: no data</div>
    );
  }

  return (
    <div>
      <div className="text-sm font-semibold text-gray-600 mb-1 text-center">{title}</div>
      <ResponsiveContainer width="100%" height={180}>
        <PieChart>
          <Pie
            data={slices}
            dataKey="value"
            nameKey="name"
            cx="50%"
            cy="50%"
            outerRadius={70}
            innerRadius={25}
            isAnimationActive={false}
          >
            {slices.map((s, i) => (
              <Cell
                key={i}
                fill={COLORS[i % COLORS.length]}
                style={onSliceClick && !s.name.startsWith("Other (") ? { cursor: "pointer" } : undefined}
                onClick={() => onSliceClick && !s.name.startsWith("Other (") && onSliceClick(s.name)}
              />
            ))}
          </Pie>
          <Tooltip
            formatter={(value: number) => value.toLocaleString()}
          />
        </PieChart>
      </ResponsiveContainer>
      <div style={{ maxHeight: "120px", overflowY: "auto", fontSize: "11px", lineHeight: "1.6" }}>
        {slices.map((s, i) => {
          const label = s.name.length > 30 ? s.name.slice(0, 28) + "\u2026" : s.name;
          const clickable = onSliceClick && !s.name.startsWith("Other (");
          return (
            <div
              key={i}
              className="flex items-center gap-1"
              title={s.name}
              style={clickable ? { cursor: "pointer" } : undefined}
              onClick={() => clickable && onSliceClick!(s.name)}
            >
              <span
                style={{
                  display: "inline-block",
                  width: 8,
                  height: 8,
                  borderRadius: "50%",
                  backgroundColor: COLORS[i % COLORS.length],
                  flexShrink: 0,
                }}
              />
              <span className={clickable ? "text-blue-600 hover:underline truncate" : "text-gray-600 truncate"}>
                {label} ({formatCount(s.value)})
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
