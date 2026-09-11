"use client";

import { create } from "zustand";
import type { User } from "@/types";
import { persist, createJSONStorage } from "zustand/middleware";

type AuthState = {
    user: User | null;
    isAuthenticated: boolean;
    isCheckingAuth: boolean;
    setUser: (user: User | null) => void;
    clearUser: () => void;
    setCheckingAuth: (isChecking: boolean) => void;
};

export const useAuthStore = create<AuthState>()(
    persist(
        (set) => ({
            user: null,
            isAuthenticated: false,
            isCheckingAuth: true,
            setUser: (user) =>
                set({
                    user,
                    isAuthenticated: !!user,
                    isCheckingAuth: false,
                }),
            clearUser: () =>
                set({
                    user: null,
                    isAuthenticated: false,
                    isCheckingAuth: false,
                }),
            setCheckingAuth: (isChecking) =>
                set({
                    isCheckingAuth: isChecking,
                }),
        }),
        {
            name: "auth",
            storage: createJSONStorage(() => localStorage),
            partialize: (state) => ({
                user: state.user,
                isAuthenticated: state.isAuthenticated,
            }),
        }
    )
);