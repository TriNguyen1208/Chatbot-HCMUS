import axios, { AxiosRequestConfig } from "axios";
import { env } from "@/config/env";
import { useAuthStore } from "@/features/auth/stores/authStore";
import type { ApiResponse } from "@/types/api.types";
import { retryWithBackoff } from "@/shared/utils/retry.util";

const BASE_URL = env.apiUrl;

export const api = axios.create({
    baseURL: BASE_URL + "/api",
    headers: { "Content-Type": "application/json" },
    withCredentials: true,
});

// Cơ chế refreshToken
let isRefreshing = false;
let failedQueue: Array<{
    resolve: (value: unknown) => void;
    reject: (reason?: unknown) => void;
}> = [];

function processQueue(error: unknown) {
    failedQueue.forEach((prom) => {
        if (error) prom.reject(error);
        else prom.resolve(null);
    });
    failedQueue = [];
}

function handleForceLogout() {
    // Xoá hết state trong localstorage
    useAuthStore.getState().clearUser();
    // Chuyển về trang login
    if (typeof window !== "undefined" && window.location.pathname !== "/") {
        window.location.href = "/";
    }
}

api.interceptors.response.use(
    (response) => response,
    async (error) => {
        const originalRequest = error.config;
        if (
            error.response &&
            error.response?.status === 401 &&
            !originalRequest._retry
        ) {
            // Đang có 1 tiến trình xin refresh token chạy rồi
            if (isRefreshing) {
                return new Promise((resolve, reject) => {
                    // Nhét request này vào hàng đợi chờ refresh xong
                    failedQueue.push({ resolve, reject });
                })
                    .then(() => {
                        // Khi được giải phóng, trình duyệt tự đính kèm cookie mới, chỉ cần gọi lại request gốc
                        return api(originalRequest);
                    })
                    .catch((err) => {
                        return Promise.reject(err);
                    });
            }
            originalRequest._retry = true;
            isRefreshing = true;

            try {
                // Áp dụng retryWithBackoff khi refresh token gặp sự cố mạng hoặc server 5xx tạm thời
                await retryWithBackoff(
                    async () => {
                        return await axios.post(
                            `${BASE_URL}/api/auth/refresh-token`,
                            {},
                            {
                                withCredentials: true,
                                timeout: 5000,
                            },
                        );
                    },
                    {
                        maxRetries: 2,
                        baseDelayMs: 1000,
                        maxDelayMs: 3000,
                        shouldRetry: (err: unknown) => {
                            if (axios.isAxiosError(err)) {
                                const status = err.response?.status;
                                // Không retry nếu là 401 hoặc 403 (Refresh token thực sự hết hạn hoặc bị thu hồi)
                                if (status === 401 || status === 403) {
                                    return false;
                                }
                                // Retry khi lỗi mạng rớt gói tin hoặc máy chủ 5xx
                                return !err.response || (status !== undefined && status >= 500) || err.code === "ECONNABORTED";
                            }
                            return true;
                        },
                        onRetry: (attempt, err, delay) => {
                            console.warn(
                                `[AxiosInterceptor] ⚠️ Refresh token attempt ${attempt} failed, retrying in ${delay}ms...`,
                                err,
                            );
                        },
                    },
                );

                // Báo cho các request đang chờ biết là refresh xong rồi
                processQueue(null);
                // Gọi lại request ban đầu (trình duyệt tự đính cookie access_token mới vào)
                return api(originalRequest);
            } catch (err: unknown) {
                processQueue(err);
                // Chỉ cưỡng chế logout khi server phản hồi mã 401 hoặc 403 (Token đã vô hiệu)
                if (axios.isAxiosError(err)) {
                    const status = err.response?.status;
                    if (status === 401 || status === 403) {
                        console.warn("[AxiosInterceptor] Refresh token expired or revoked. Logging out...");
                        handleForceLogout();
                    }
                }
                return Promise.reject(err);
            } finally {
                isRefreshing = false;
            }
        }
        return Promise.reject(error);
    },
);

/**
 * Standardized HTTP client helper that automatically unwraps `res.data.data` from backend `ApiResponse<T>`
 */
export const http = {
    get: async <T>(url: string, config?: AxiosRequestConfig): Promise<T> => {
        const res = await api.get<ApiResponse<T>>(url, config);
        return res.data.data;
    },
    post: async <T>(
        url: string,
        body?: unknown,
        config?: AxiosRequestConfig,
    ): Promise<T> => {
        const res = await api.post<ApiResponse<T>>(url, body, config);
        return res.data.data;
    },
    put: async <T>(
        url: string,
        body?: unknown,
        config?: AxiosRequestConfig,
    ): Promise<T> => {
        const res = await api.put<ApiResponse<T>>(url, body, config);
        return res.data.data;
    },
    patch: async <T>(
        url: string,
        body?: unknown,
        config?: AxiosRequestConfig,
    ): Promise<T> => {
        const res = await api.patch<ApiResponse<T>>(url, body, config);
        return res.data.data;
    },
    delete: async <T>(url: string, config?: AxiosRequestConfig): Promise<T> => {
        const res = await api.delete<ApiResponse<T>>(url, config);
        return res.data.data;
    },
};
