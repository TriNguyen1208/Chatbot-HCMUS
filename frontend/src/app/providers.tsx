"use client";

import { GoogleOAuthProvider } from "@react-oauth/google";
import { env } from "@/config/env";

import { useEffect } from "react";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { authApi } from "@/features/auth/services/authApi";

export function Providers({ children }: { children: React.ReactNode }) {
  const { setUser, setCheckingAuth } = useAuthStore();

  useEffect(() => {
    let isMounted = true;
    const checkAuth = async () => {
      try {
        const user = await authApi.getMe();
        console.log(user)
        if (isMounted) setUser(user);
      } catch (err) {
        if (isMounted) setCheckingAuth(false);
      }
    };
    checkAuth();
    return () => {
      isMounted = false;
    };
  }, [setUser, setCheckingAuth]);

  return (
    <GoogleOAuthProvider clientId={env.googleClientId}>
      <div className="text-gray-900 antialiased">{children}</div>
    </GoogleOAuthProvider>
  );
}