"use client";

import { ApiResponse } from "@/types/type";
import { api } from "@/lib/api";
import { UserProfile } from "../types";

export const authApi = {
    googleLogin: async (idToken: string): Promise<Pick<UserProfile, "id" | "name" | "email" | "avatar_url" | "role">> => {
        const res = await api.post<ApiResponse<Pick<UserProfile, "id" | "name" | "email" | "avatar_url" | "role">>>(`/auth/google`, { idToken });
        return res.data.data;
    },
    microsoftLogin: async (idToken: string): Promise<Pick<UserProfile, "id" | "name" | "email" | "avatar_url" | "role">> => {
        const res = await api.post<ApiResponse<Pick<UserProfile, "id" | "name" | "email" | "avatar_url" | "role">>>(`/auth/microsoft`, { idToken });
        return res.data.data;
    },
    logout: async (): Promise<void> => {
        await api.post(`/auth/logout`);
    },
    logoutAll: async (): Promise<void> => {
        await api.post(`/auth/logout-all`);
    },
    getMe: async (): Promise<Pick<UserProfile, "id" | "name" | "email" | "avatar_url" | "role"> & { studentID?: string, student_id?: string }> => {
        const res = await api.get<ApiResponse<Pick<UserProfile, "id" | "name" | "email" | "avatar_url" | "role"> & { studentID?: string, student_id?: string }>>(`/user/me`);
        return res.data.data;
    }
};