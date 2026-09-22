"use client";

import { SocketProvider } from "@/providers/SocketProvider";
import { useEffect } from "react";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { authApi } from "@/features/auth/api/authApi";

export default function MainLayout({
    children,
}: Readonly<{
    children: React.ReactNode;
}>) {
    const { isAuthenticated, isCheckingAuth, setUser, clearUser, setCheckingAuth } = useAuthStore();

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
    if (isCheckingAuth) {
        return (
            <div className="w-screen h-screen flex flex-col items-center justify-center bg-background gap-3">
                <div className="w-8 h-8 border-3 border-brand-primary border-t-transparent rounded-full animate-spin" />
                <p className="text-sm text-foreground/60 font-medium">Đang khởi tạo phiên làm việc...</p>
            </div>
        );
    }
    // 3. Nếu kiểm tra xong mà không hợp lệ -> chặn không render
    if (!isAuthenticated) {
        return null;
    }
    return <SocketProvider>{children}</SocketProvider>;
}
