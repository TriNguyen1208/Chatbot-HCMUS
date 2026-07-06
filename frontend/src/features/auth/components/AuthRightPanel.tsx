"use client"
import { GoogleLogin } from "@react-oauth/google";
import { useGoogleAuth } from "../hooks/useGoogleAuth";
import { ALLOWED_DOMAINS } from "@/config/constant";

export function AuthRightPanel(){
    const {status, errorMsg, handleGoogleSuccess, handleGoogleError, resetStatus } = useGoogleAuth()

    return (
        <div className="flex-1 flex items-center justify-center p-8 bg-white">
            <div className="w-full max-w-sm">
                <div className="flex items-center gap-2 mb-10 lg:hidden">
                    <div className="w-8 h-8 rounded-lg bg-[#2563EB] flex items-center justify-center">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none"><path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" /></svg>
                    </div>
                    <span className="font-semibold text-gray-900">SchoolConnect</span>
                </div>

                <h2 className="text-2xl font-bold text-gray-900 mb-1">Chào mừng bạn</h2>
                <p className="text-gray-500 text-sm mb-8">Đăng nhập bằng email trường để tiếp tục</p>

                {status === "error" && (
                    <div className="mb-6 p-4 rounded-xl bg-red-50 border border-red-100 flex gap-3">
                        <svg className="w-5 h-5 text-red-500 flex-shrink-0 mt-0.5" viewBox="0 0 24 24" fill="none"><circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="2" /><path d="M12 8v4M12 16h.01" stroke="currentColor" strokeWidth="2" strokeLinecap="round" /></svg>
                        <p className="text-red-700 text-sm">{errorMsg}</p>
                    </div>
                )}

                <div onClick={resetStatus}>
                    <GoogleLogin onSuccess={handleGoogleSuccess} onError={handleGoogleError} theme="outline" shape="pill" width="350" />
                </div>

                <div className="my-6 flex items-center gap-3">
                    <div className="flex-1 h-px bg-gray-100" /><span className="text-xs text-gray-400">Chỉ chấp nhận email trường</span><div className="flex-1 h-px bg-gray-100" />
                </div>

                <div className="rounded-xl bg-gray-50 border border-gray-100 p-4">
                    <p className="text-xs font-medium text-gray-500 mb-3 uppercase tracking-wide">Email được chấp nhận</p>
                    <div className="flex flex-wrap gap-2">
                        {ALLOWED_DOMAINS.map((d) => (
                            <span key={d} className="px-2.5 py-1 rounded-lg bg-white border border-gray-200 text-xs text-gray-600 font-mono">*{d}</span>
                        ))}
                    </div>
                </div>

                <p className="mt-6 text-center text-xs text-gray-400">
                    Bằng cách đăng nhập, bạn đồng ý với <a href="#" className="text-[#2563EB] hover:underline">Điều khoản sử dụng</a> và <a href="#" className="text-[#2563EB] hover:underline">Chính sách bảo mật</a>
                </p>
            </div>
        </div>
    )
}

