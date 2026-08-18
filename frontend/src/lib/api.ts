import axios, { InternalAxiosRequestConfig } from "axios";
import { env } from "@/config/env"
import { useAuthStore } from "@/features/auth/stores/authStore"

const BASE_URL = env.apiUrl;

export const api = axios.create({
    baseURL: BASE_URL + "/api",
    headers: { "Content-Type": "application/json" },
    withCredentials: true
})

//Cơ chế refreshToken
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
    //Xoá hết state trong localstorage
    useAuthStore.getState().clearUser();
    //Chuyển về trang login
    if (typeof window !== "undefined" && window.location.pathname !== "/") {
        window.location.href = "/";
    }
}

api.interceptors.response.use((response) => response, async (error) => {
    const originalRequest = error.config;
    if (error.response && error.response?.status === 401 && !originalRequest._retry) {
        // Đang có 1 tiến trình xin refresh token chạy rồi
        if (isRefreshing) {
            return new Promise((resolve, reject) => {
                // Nhét request này vào hàng đợi chờ refresh xong
                failedQueue.push({ resolve, reject });
            }).then(() => {
                // Khi được giải phóng, trình duyệt tự đính kèm cookie mới, chỉ cần gọi lại request gốc
                return api(originalRequest);
            }).catch(err => {
                return Promise.reject(err);
            });
        }
        originalRequest._retry = true;
        isRefreshing = true;

        try {
            // Trình duyệt tự gửi HttpOnly refreshToken đi
            await axios.post(`${BASE_URL}/api/auth/refresh-token`, {}, {
                withCredentials: true
            });
            // Báo cho các request đang chờ biết là refresh xong rồi
            processQueue(null);
            // Gọi lại request ban đầu (trình duyệt tự đính cookie access_token mới vào)
            return api(originalRequest);
        } catch (err) {
            // Refresh token thất bại (hết hạn hoặc server báo lỗi)
            processQueue(err);
            //Ép buộc phải đăng nhập lại
            handleForceLogout();
            return Promise.reject(err);
        } finally {
            isRefreshing = false;
        }
    }
    return Promise.reject(error);
})