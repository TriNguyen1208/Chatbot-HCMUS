"use client"
import { useRouter } from "next/navigation";
import { useAuthStore } from "@/features/auth";
import { useState } from "react";
import { authApi } from "@/features/auth";
import { setTokens } from "@/features/auth";
import axios from "axios";

export function useGoogleAuth(){
    const router = useRouter()
    const setUser = useAuthStore((s) => s.setUser)
    const [status, setStatus] = useState<"idle" | "loading" | "error">("idle");
    const [errorMsg, setErrorMsg] = useState("");

    const handleGoogleSuccess = async (credentialResponse: any) => {
        const idToken = credentialResponse.credential;
        if(!idToken){
            setStatus("error")
            setErrorMsg("Không nhận được thông tin xác thực từ Google.")
        }

        setStatus("loading");
        setErrorMsg("");

        try{
            const result = await authApi.googleLogin(idToken);
            setTokens(result.tokens);
            setUser(result.user)
            router.push("/chat")
        }catch(err){
            if (axios.isAxiosError(err)) {
                setErrorMsg(err.response?.status === 401 
                    ? "Email của bạn không thuộc trường. Vui lòng dùng email trường để đăng nhập." 
                    : err.response?.data?.message || "Đăng nhập thất bại"
                );
            }else {
                setErrorMsg("Có lỗi xảy ra, vui lòng thử lại.");
            }
            setStatus("error")
        }
    }
    const handleGoogleError = () => {
        setErrorMsg("Không thể kết nối với Google. Vui lòng thử lại.");
        setStatus("error")
    }

    const resetStatus = () => {
        setStatus("idle");
        setErrorMsg("");
    };
    return {status, errorMsg, handleGoogleSuccess, handleGoogleError, resetStatus}
}

