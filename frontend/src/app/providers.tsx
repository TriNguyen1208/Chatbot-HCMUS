"use client";

import { GoogleOAuthProvider } from "@react-oauth/google";
import { env } from "@/config/env";

import { useEffect, useState } from "react";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useRouter, usePathname } from "next/navigation";
import { authApi } from "@/features/auth/api/authApi";
import { QueryProvider } from "@/providers/QueryProvider";
import { SocketProvider } from "@/providers/SocketProvider";
import { ThemeProvider } from "next-themes";

export function Providers({ children }: { children: React.ReactNode }) {
  const { isCheckingAuth, isAuthenticated, setCheckingAuth, setUser, clearUser } = useAuthStore();
  const router = useRouter();
  const pathname = usePathname();
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  // 1. Kiểm tra xác thực (gọi API /user/me) duy nhất 1 lần khi App khởi tạo
  useEffect(() => {
    let isMounted = true;
    const initAuth = async () => {
      try {
        const user = await authApi.getMe();
        if (isMounted) {
          setUser(user);
        }
      } catch (err) {
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
    // Chỉ chạy 1 lần khi mount
  }, [setUser, clearUser, setCheckingAuth]);

  // 2. Lắng nghe thay đổi trạng thái xác thực để điều hướng (Routing)
  useEffect(() => {
    // Đợi check xong mới thực hiện điều hướng
    if (isCheckingAuth) return;

    if (isAuthenticated) {
      if (pathname === "/") {
        router.push("/chat");
      }
    } else {
      if (pathname !== "/") {
        router.push("/");
      }
    }
  }, [isAuthenticated, isCheckingAuth, pathname, router]);

  if (!mounted) {
    return null;
  }

  return (
    <ThemeProvider attribute="class" defaultTheme="system" enableSystem>
      <GoogleOAuthProvider clientId={env.googleClientId}>
        <QueryProvider>
          <SocketProvider>
            <div className="text-foreground antialiased w-full h-full">
              {/* Nếu muốn màn hình loading lúc check auth thì có thể check isCheckingAuth ở đây */}
              {children}
            </div>
          </SocketProvider>
        </QueryProvider>
      </GoogleOAuthProvider>
    </ThemeProvider>
  );
}