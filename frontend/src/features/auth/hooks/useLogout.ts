"use client"

import { authApi } from "../api/authApi";

import { useAuthStore } from "../stores/authStore";
import { useRouter } from "next/navigation";
import { useEffect, useState } from "react";

export function useLogout() {
    const { user, isAuthenticated, isCheckingAuth, clearUser } = useAuthStore()
    const router = useRouter()

    useEffect(() => {
        console.log(`[useLogout] Effect run. isCheckingAuth=${isCheckingAuth}, isAuthenticated=${isAuthenticated}`);
        if (!isCheckingAuth && !isAuthenticated) {
            console.log(`[useLogout] REDIRECTING TO /`);
            router.replace("/");
        }
    }, [isAuthenticated, isCheckingAuth, user, router])

    const handleLogout = async () => {
        clearUser();
        await authApi.logout()
        router.replace("/");
    }
    return {
        user: !isCheckingAuth ? user : null,
        handleLogout
    }
}