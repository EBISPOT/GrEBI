import React from "react";
import { Link, useLocation } from "react-router-dom";
import ErrorMessage from "./ErrorMessage";

type Props = { children: React.ReactNode; resetKey?: string };
type State = { error: any };

/**
 * Shows an error thrown while rendering the page in place of the page, so a
 * bug in one component does not blank the whole app. The next navigation
 * (a new resetKey) clears it.
 */
export default class ErrorBoundary extends React.Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: any): State {
    return { error };
  }

  componentDidCatch(error: any, info: React.ErrorInfo) {
    console.error("Error rendering the page", error, info.componentStack);
  }

  componentDidUpdate(prev: Props) {
    if (prev.resetKey !== this.props.resetKey && this.state.error) {
      this.setState({ error: null });
    }
  }

  render() {
    if (this.state.error) {
      return (
        <main className="container mx-auto px-4 my-8">
          <ErrorMessage error={this.state.error} what="This page" />
          <Link className="link-default" to="/">Return to home</Link>
        </main>
      );
    }
    return this.props.children;
  }
}

/** The boundary around the routed pages: navigating away clears the error. */
export function RouteErrorBoundary({ children }: { children: React.ReactNode }) {
  const location = useLocation();
  return <ErrorBoundary resetKey={location.key}>{children}</ErrorBoundary>;
}
