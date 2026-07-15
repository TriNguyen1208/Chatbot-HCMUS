"use client";

import { useEffect, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { useAuthStore } from "../stores/authStore";
import { AuthLeftPanel } from "./AuthLeftPanel";
import { AuthRightPanel } from "./AuthRightPanel";

export function LoginForm() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const { isAuthenticated } = useAuthStore();
  const [hydrated, setHydrated] = useState(false);

  useEffect(() => {
    setHydrated(true);
  }, []);

  //Neu nhu da dang nhap thi phai vao trang home
  useEffect(() => {
    if (hydrated && isAuthenticated) {
      const url = searchParams.get("callbackUrl") || "/home"
      router.replace(url)
    }
  }, [hydrated, isAuthenticated, router, searchParams]);

  // Ngăn chặn render giao diện login nếu đã đăng nhập để tránh chớp nhoáng
  if (hydrated && isAuthenticated) {
    return null;
  }

  return (
    <div className="min-h-screen flex flex-col lg:flex-row w-full">
      <AuthLeftPanel />
      <AuthRightPanel />
    </div>
  );
}