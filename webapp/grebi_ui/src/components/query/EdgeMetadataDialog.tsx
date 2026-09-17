import { Dialog, DialogTitle, DialogContent, IconButton } from "@mui/material";
import { Close, OpenInNew } from "@mui/icons-material";
import { Link } from "react-router-dom";
import EdgeDetails, { edgePageUrl } from "../EdgeDetails";

interface EdgeMetadataDialogProps {
  open: boolean;
  onClose: () => void;
  graph: string;
  edgeId: string | null;
}

/** An edge's details in a dialog, with a link to the edge's own page. */
export default function EdgeMetadataDialog({
  open,
  onClose,
  graph,
  edgeId,
}: EdgeMetadataDialogProps) {
  if (!open) return null;

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle className="flex justify-between items-center">
        <span>
          Edge Properties
          {edgeId && (
            <Link className="link-default text-sm font-normal ml-3 inline-flex items-center gap-1" to={edgePageUrl(graph, edgeId)} onClick={onClose}>
              <OpenInNew fontSize="inherit" /> Open edge page
            </Link>
          )}
        </span>
        <IconButton onClick={onClose} size="small">
          <Close />
        </IconButton>
      </DialogTitle>
      <DialogContent dividers>
        {edgeId && <EdgeDetails graph={graph} edgeId={edgeId} />}
      </DialogContent>
    </Dialog>
  );
}
