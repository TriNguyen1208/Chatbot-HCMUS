"use client";

import { create } from "zustand"
import type { UserProfile } from "../types";
import { persist, createJSONStorage } from "zustand/middleware";

type AuthState = {
    user: Pick<UserProfile, "id" | "name" | "email"> & { studentID?: string } | null;
    isAuthenticated: boolean,
    isCheckingAuth: boolean,
    setUser: (user: AuthState["user"]) => void
    clearUser: () => void,
    setCheckingAuth: (isChecking: boolean) => void
}

export const useAuthStore = create<AuthState>()(
    persist(
        (set) => ({
            user: null,
            isAuthenticated: false,
            isCheckingAuth: true,
            setUser: (user) => set({
                user: user,
                isAuthenticated: !!user,
                isCheckingAuth: false
            }),
            clearUser: () => set({
                user: null,
                isAuthenticated: false,
                isCheckingAuth: false
            }),
            setCheckingAuth: (isChecking) => set({
                isCheckingAuth: isChecking
            })
        }),
        {
            name: "auth",
            storage: createJSONStorage(() => localStorage),
            partialize: (state) => ({
                user: state.user,
                isAuthenticated: state.isAuthenticated
            }),
        }
    )
)