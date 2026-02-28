import { Dialog, DialogTitle, DialogContent, IconButton } from "@mui/material";
import { Close } from "@mui/icons-material";

interface EdgeMetadataDialogProps {
  open: boolean;
  onClose: () => void;
  data: Record<string, any> | null;
}

function formatValue(value: any): string {
  if (value === null || value === undefined) return "";
  if (Array.isArray(value)) {
    return value.map(v => String(v)).join(", ");
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

export default function EdgeMetadataDialog({ open, onClose, data }: EdgeMetadataDialogProps) {
  if (!data) return null;

  const entries = Object.entries(data).sort(([a], [b]) => a.localeCompare(b));

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle className="flex justify-between items-center">
        <span>Edge Properties</span>
        <IconButton onClick={onClose} size="small">
          <Close />
        </IconButton>
      </DialogTitle>
      <DialogContent dividers>
        <table className="table-auto border-collapse w-full text-sm">
          <thead>
            <tr className="border-b-2 border-grey-default">
              <td className="font-bold py-2 px-3">Property</td>
              <td className="font-bold py-2 px-3">Value</td>
            </tr>
          </thead>
          <tbody>
            {entries.map(([key, value]) => (
              <tr key={key} className="even:bg-grey-50 border-b border-grey-default">
                <td className="py-2 px-3 font-mono whitespace-nowrap align-top">{key}</td>
                <td className="py-2 px-3 break-all">{formatValue(value)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </DialogContent>
    </Dialog>
  );
}
