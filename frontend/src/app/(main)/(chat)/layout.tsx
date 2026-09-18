"use client";
import { Sidebar } from "@/features/chat/components";
import { useChatSocket } from "@/features/chat/hooks/useChatSocket";
import { useState, useRef, useEffect } from "react";

const Layout = ({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) => {
  useChatSocket();

  const [sidebarWidth, setSidebarWidth] = useState(300);
  const isResizing = useRef(false);

  const startResizing = (e: React.MouseEvent) => {
    isResizing.current = true;
    document.body.style.cursor = "col-resize";
  };

  const stopResizing = () => {
    isResizing.current = false;
    document.body.style.cursor = "default";
  };

  const resize = (e: MouseEvent) => {
    if (isResizing.current) {
      const newWidth = Math.max(220, Math.min(e.clientX, 600));
      setSidebarWidth(newWidth);
    }
  };

  useEffect(() => {
    window.addEventListener("mousemove", resize);
    window.addEventListener("mouseup", stopResizing);
    return () => {
      window.removeEventListener("mousemove", resize);
      window.removeEventListener("mouseup", stopResizing);
    };
  }, []);

  return (
    <main className="flex w-screen h-screen overflow-hidden bg-background">
      <div style={{ width: sidebarWidth }} className="h-full shrink-0 overflow-hidden flex flex-col">
        <Sidebar />
      </div>

      {/* Resizer Handle */}
      <div
        className="w-1 cursor-col-resize bg-glass-border hover:bg-brand-primary active:bg-brand-primary transition-colors duration-150 z-50 shrink-0"
        onMouseDown={startResizing}
      />

      <div className="flex-1 h-full overflow-hidden min-w-0">
        {children}
      </div>
    </main>
  );
};

export default Layout;
