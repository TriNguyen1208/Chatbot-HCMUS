"use client";

import { GoogleOAuthProvider } from "@react-oauth/google";
import { env } from "@/config/env";
import { useEffect } from "react";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { authApi } from "@/features/auth/api/authApi";
import { QueryProvider } from "@/providers/QueryProvider";
import { ThemeProvider } from "next-themes";

export function Providers({ children }: { children: React.ReactNode }) {
    return (
        <ThemeProvider attribute="class" defaultTheme="system" enableSystem>
            <GoogleOAuthProvider clientId={env.googleClientId || ""}>
                <QueryProvider>
                    <div className="text-foreground antialiased w-full h-full">
                        {children}
                    </div>
                </QueryProvider>
            </GoogleOAuthProvider>
        </ThemeProvider>
    );
}
