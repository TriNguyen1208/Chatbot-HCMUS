"use client";

import { GoogleOAuthProvider } from "@react-oauth/google";
import { env } from "@/config/env";

import { useEffect } from "react";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useRouter, usePathname } from "next/navigation";

export function Providers({ children }: { children: React.ReactNode }) {
  const { setCheckingAuth } = useAuthStore();
  const router = useRouter();
  const pathname = usePathname();

  useEffect(() => {
    let isMounted = true;
    const checkAuth = () => {
      try {
        const authDataStr = localStorage.getItem("auth");
        let hasUser = false;
        
        if (authDataStr) {
          const authData = JSON.parse(authDataStr);
          if (authData?.state?.user) {
            hasUser = true;
          }
        }

        if (isMounted) {
          if (hasUser) {
            if (pathname === "/") {
              router.push("/chat");
            }
          } else {
            if (pathname !== "/") {
              router.push("/");
            }
          }
          setCheckingAuth(false);
        }
      } catch (err) {
        if (isMounted) {
          setCheckingAuth(false);
          if (pathname !== "/") {
            router.push("/");
          }
        }
      }
    };
    checkAuth();
    
    return () => {
      isMounted = false;
    };
  }, [router, pathname, setCheckingAuth]);
  return (
    <GoogleOAuthProvider clientId={env.googleClientId}>
      <div className="text-gray-900 antialiased">{children}</div>
    </GoogleOAuthProvider>
  );
}