"use client"
import { useRouter } from "next/navigation";
import { useAuthStore } from "../stores/authStore";
import { useState, useEffect } from "react";
import { authApi } from "../api/authApi";
import { PublicClientApplication } from "@azure/msal-browser";
import axios from "axios";
import { env } from "@/config/env"
// Khởi tạo MSAL instance bên ngoài hook để tái sử dụng
const msalConfig = {
    auth: {
        clientId: env.microsoftClientId as string,
        authority: `https://login.microsoftonline.com/${env.microsoftTenantId}`,
        redirectUri: env.baseUrl // Đảm bảo đã khai báo trên Azure Portal
    },
    cache: {
        cacheLocation: "sessionStorage",
        storeAuthStateInCookie: false,
    }
};

const msalInstance = new PublicClientApplication(msalConfig);

let msalInitPromise: Promise<void> | null = null;

export function useMicrosoftAuth() {
    const router = useRouter()
    const setUser = useAuthStore((s) => s.setUser)
    const [status, setStatus] = useState<"idle" | "loading" | "error">("idle");
    const [errorMsg, setErrorMsg] = useState("");

    const [isInitialized, setIsInitialized] = useState(false);

    useEffect(() => {
        if (!msalInitPromise) {
            msalInitPromise = msalInstance.initialize().then(() => {
                setIsInitialized(true);
                // Xử lý kết quả trả về từ Microsoft sau khi redirect về trang web
                return msalInstance.handleRedirectPromise();
            }).then((response) => {
                if (response && response.accessToken) {
                    setStatus("loading");
                    return authApi.microsoftLogin(response.accessToken).then((result) => {
                        setUser(result);
                        router.push("/chat");
                    });
                }
            }).catch(e => {
                console.error("MSAL init/redirect error:", e);
                msalInitPromise = null;
                if (e.message && e.message.includes("interaction_in_progress")) {
                    sessionStorage.clear();
                } else if (axios.isAxiosError(e)) {
                    setErrorMsg(e.response?.status === 401 ? "Email của bạn không hợp lệ." : "Đăng nhập thất bại");
                }
                setStatus("error");
            });
        } else {
            msalInitPromise.then(() => setIsInitialized(true));
        }
    }, [router, setUser]);

    const loginWithMicrosoft = async () => {
        if (!isInitialized) {
            setErrorMsg("Hệ thống đang khởi tạo, vui lòng chờ...");
            return;
        }

        try {
            setStatus("loading");
            setErrorMsg("");

            const loginRequest = {
                scopes: ["user.read"]
            };

            // Dùng loginRedirect thay vì loginPopup để tránh bị chặn hoặc kẹt popup
            await msalInstance.loginRedirect(loginRequest);

        } catch (err) {
            if (err instanceof Error) {
                console.error("Lỗi đăng nhập Microsoft chi tiết:", err.message, err.stack);
                setErrorMsg(`Lỗi MSAL: ${err.message}`);
            } else {
                setErrorMsg("Có lỗi không xác định xảy ra, vui lòng thử lại.");
            }
            setStatus("error");
        }
    }
    const resetStatus = () => {
        setStatus("idle");
        setErrorMsg("");
    };

    return { status, errorMsg, loginWithMicrosoft, resetStatus }
}
