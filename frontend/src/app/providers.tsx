"use client";

import { GoogleOAuthProvider } from "@react-oauth/google";
import { env } from "@/config/env";
import { useEffect } from "react";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { authApi } from "@/features/auth/api/authApi";
import { QueryProvider } from "@/providers/QueryProvider";
import { ThemeProvider } from "next-themes";

export function Providers({ children }: { children: React.ReactNode }) {
  const { setUser, clearUser, setCheckingAuth } = useAuthStore();

  useEffect(() => {
    let isMounted = true;
    const initAuth = async () => {
      try {
        const user = await authApi.getMe();
        if (isMounted) {
          setUser(user);
        }
      } catch {
        if (isMounted) {
          clearUser();
        }
      } finally {
        if (isMounted) {
          setCheckingAuth(false);
        }
      }
    };

    initAuth();

    return () => {
      isMounted = false;
    };
  }, [setUser, clearUser, setCheckingAuth]);

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