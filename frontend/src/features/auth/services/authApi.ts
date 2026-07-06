"use client";

import { AuthResult } from "@/features/auth";
import { ApiResponse } from "@/types/type";
import { env } from "@/config/env";
import axios from "axios";

const BASE_URL = env.apiUrl;

export const authApi = {
    googleLogin: async (idToken: string): Promise<AuthResult> => {
        const res = await axios.post<ApiResponse<AuthResult>>(`${BASE_URL}/auth/google`, { idToken });
        return res.data.data;
    },
    logout: async (refreshToken: string): Promise<void> => {
        await axios.post(`${BASE_URL}/auth/logout`, { refreshToken: refreshToken });
    },
};