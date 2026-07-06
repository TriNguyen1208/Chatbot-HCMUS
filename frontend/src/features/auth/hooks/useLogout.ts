"use client"

import { authApi, clearTokens, useAuthStore } from "@/features/auth";
import { useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import Cookies from "js-cookie";

export function useLogout(){
    const {user, isAuthenticated, clearUser} = useAuthStore()
    const router = useRouter()
    const [hydrated, setHydrated] = useState(false); //Da san sang de check chua

    useEffect(() => {
        setHydrated(true)
    }, [])

    useEffect(() => {
        if (hydrated && (!isAuthenticated || !Cookies.get("accessToken"))) { //Neu nhu khong co accessToken hoac khong authenticate thi ve root
            router.replace("/");
        }
    }, [isAuthenticated, hydrated, user])

    const handleLogout = () => {
        const refreshToken = Cookies.get("refreshToken");
        clearTokens();
        clearUser();

        if(refreshToken){
            authApi.logout(refreshToken).catch(() => {})
        }

        router.replace("/");
    }
    return {
        user: hydrated ? user : null,
        handleLogout
    }
}