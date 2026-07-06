"use client";
import { useLogout } from "@/features/auth";

export function HomePlaceholder() {
  const { user } = useLogout();
  if (!user) return null;

  return (
    <div className="rounded-2xl bg-white border border-gray-100 p-8 text-center">
        <div className="w-16 h-16 rounded-2xl bg-[#2563EB]/10 flex items-center justify-center mx-auto mb-4">
            <svg width="28" height="28" viewBox="0 0 24 24" fill="none"><path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5" stroke="#2563EB" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" /></svg>
        </div>
        <h2 className="text-xl font-bold text-gray-900 mb-2">Chào mừng, {user.name?.split(" ").pop()}! 👋</h2>
        <p className="text-gray-500 text-sm mb-1">{user.email}</p>
        {user.studentID && <p className="text-gray-400 text-xs">MSSV: {user.studentID}</p>}
        <div className="mt-6 p-4 rounded-xl bg-blue-50 border border-blue-100">
            <p className="text-blue-700 text-sm">🚀 Bảng tin, tin nhắn và các tính năng khác đang được phát triển.</p>
        </div>
    </div>
  );
}