"use client";

import {create} from "zustand"
import { persist } from "zustand/middleware"
import type { UserProfile } from "@/features/auth/index";

type AuthState = {
    user: Pick<UserProfile, "id" | "name" | "email"> & { studentID?: string } | null;
    isAuthenticated: boolean,
    setUser: (user: AuthState["user"]) => void
    clearUser: () => void
}

export const useAuthStore = create<AuthState>()(
    persist( //Khong bi mat khi F5 trang (Giup du lieu luu xuong localstorage)
        (set) => ({
            user: null,
            isAuthenticated: false,
            setUser: (user) => set({
                user: user,
                isAuthenticated: !!user
            }),
            clearUser: () => set({
                user: null,
                isAuthenticated: false
            })
        }), {
            name: "auth-storage",
            partialize: (state) => ({
                user: state.user,
                isAuthenticated: state.isAuthenticated
            })
        }
    )
)
