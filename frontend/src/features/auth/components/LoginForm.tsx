"use client";

import { AuthLeftPanel } from "./AuthLeftPanel";
import { AuthRightPanel } from "./AuthRightPanel";

export function LoginForm() {
  return (
    <div className="min-h-screen flex flex-col lg:flex-row w-full">
        <AuthLeftPanel />
        <AuthRightPanel />
    </div>
  );
}