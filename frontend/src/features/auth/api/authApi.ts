"use client";

import { ApiResponse } from "@/types/type";
import { api } from "@/lib/api";
import { UserProfile } from "../types";

export const authApi = {
    googleLogin: async (idToken: string): Promise<Pick<UserProfile, "id" | "name" | "email" | "avatar_url">> => {
        const res = await api.post<ApiResponse<Pick<UserProfile, "id" | "name" | "email" | "avatar_url">>>(`/auth/google`, { idToken });
        return res.data.data;
    },
    microsoftLogin: async (idToken: string): Promise<Pick<UserProfile, "id" | "name" | "email" | "avatar_url">> => {
        const res = await api.post<ApiResponse<Pick<UserProfile, "id" | "name" | "email" | "avatar_url">>>(`/auth/microsoft`, { idToken });
        return res.data.data;
    },
    logout: async (): Promise<void> => {
        await api.post(`/auth/logout`);
    },
    logoutAll: async (): Promise<void> => {
        await api.post(`/auth/logout-all`);
    },
    getMe: async (): Promise<Pick<UserProfile, "id" | "name" | "email" | "avatar_url"> & { studentID?: string }> => {
        const res = await api.get<ApiResponse<Pick<UserProfile, "id" | "name" | "email" | "avatar_url"> & { studentID?: string }>>(`/user/me`);
        return res.data.data;
    }
};