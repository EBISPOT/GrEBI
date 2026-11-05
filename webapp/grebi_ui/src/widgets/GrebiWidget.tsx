import { useMemo } from "react";
import createCache from "@emotion/cache";
import { CacheProvider } from "@emotion/react";
import { ThemeProvider, createTheme } from "@mui/material/styles";
import { MemoryRouter } from "react-router-dom";
import EbiRoutes from "../frontends/ebi/EbiRoutes";
import WidgetLayout from "../frontends/ebi/WidgetLayout";
import { theme } from "../frontends/ebi/theme";

/**
 * A page of GrEBI inside a shadow root on another site: a router of its own
 * started at `path`, the widget layout around the same pages as the site, and
 * the styles MUI generates kept inside the shadow root, its popups and
 * dialogs too, so that nothing of the host's leaks in or out.
 *
 * The popups and dialogs follow the theme's default props only while the
 * bundle holds one copy of MUI. A deep import of an icon
 * (`@mui/icons-material/X`) is the package's CommonJS build and brings in a
 * second copy, with React contexts of its own that the theme never reaches;
 * icons are imported by name from `@mui/icons-material` instead, and the CI
 * checks the bundle for a second copy.
 */
export default function GrebiWidget({ path, shadowRoot, container }: { path: string; shadowRoot: ShadowRoot; container: HTMLElement }) {
  const cache = useMemo(() => createCache({ key: "grebi", container: shadowRoot }), [shadowRoot]);
  const widgetTheme = useMemo(
    () =>
      createTheme(theme, {
        components: {
          MuiPopover: { defaultProps: { container } },
          MuiPopper: { defaultProps: { container } },
          MuiModal: { defaultProps: { container } },
        },
      }),
    [container]
  );
  return (
    <CacheProvider value={cache}>
      <ThemeProvider theme={widgetTheme}>
        <MemoryRouter initialEntries={[path]} key={path}>
          <EbiRoutes layout={<WidgetLayout />} />
        </MemoryRouter>
      </ThemeProvider>
    </CacheProvider>
  );
}
