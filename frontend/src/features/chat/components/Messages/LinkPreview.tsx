"use client";

import React, { Component, ErrorInfo, ReactNode } from "react";
import dynamic from "next/dynamic";

const Microlink = dynamic(() => import("@microlink/react"), { 
  ssr: false,
  loading: () => (
    <div className="p-3 text-xs text-txt-extra flex items-center gap-2">
      <div className="size-3 rounded-full border-2 border-brand-primary border-t-transparent animate-spin" />
      <span>Đang tải xem trước liên kết...</span>
    </div>
  )
});

interface Props {
  url: string;
}

interface State {
  hasError: boolean;
}

class LinkErrorBoundary extends Component<{ children: ReactNode; fallbackUrl: string }, State> {
  state: State = { hasError: false };

  static getDerivedStateFromError() {
    return { hasError: true };
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo) {
    console.warn("Microlink preview failed to load:", error, errorInfo);
  }

  render() {
    if (this.state.hasError) {
      return null;
    }
    return this.props.children;
  }
}

export const LinkPreview: React.FC<Props> = ({ url }) => {
  if (!url) return null;

  return (
    <div 
      className="mt-2 w-full max-w-[360px] sm:max-w-[420px] rounded-2xl overflow-hidden border border-glass-border shadow-md bg-surface/60 backdrop-blur-md transition-transform hover:scale-[1.01]"
      onClick={(e) => e.stopPropagation()}
    >
      <LinkErrorBoundary fallbackUrl={url}>
        <Microlink 
          url={url} 
          size="normal"
          style={{
            width: "100%",
            borderRadius: "16px",
            border: "none",
            backgroundColor: "transparent",
            color: "inherit",
          }}
        />
      </LinkErrorBoundary>
    </div>
  );
};
