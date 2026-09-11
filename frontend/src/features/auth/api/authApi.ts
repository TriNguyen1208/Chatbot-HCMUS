import { http, api } from "@/lib/api";
import type { User } from "@/types";

export const authApi = {
    googleLogin: async (idToken: string): Promise<User> => {
        return http.post<User>(`/auth/google`, { idToken });
    },
    microsoftLogin: async (idToken: string): Promise<User> => {
        return http.post<User>(`/auth/microsoft`, { idToken });
    },
    logout: async (): Promise<void> => {
        await api.post(`/auth/logout`);
    },
    logoutAll: async (): Promise<void> => {
        await api.post(`/auth/logout-all`);
    },
    getMe: async (): Promise<User> => {
        return http.get<User>(`/user/me`);
    },
};