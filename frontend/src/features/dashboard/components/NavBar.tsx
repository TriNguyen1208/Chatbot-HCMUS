"use client";

import {useLogout} from "@/features/auth/index";

export function Navbar() {
    const {user, handleLogout} = useLogout()
    if (!user) return null;

    return (
        <header className="bg-white border-b border-gray-100 sticky top-0 z-10 w-full">
        <div className="max-w-4xl mx-auto px-4 h-14 flex items-center justify-between">
            <div><span className="font-semibold text-gray-900 text-sm">SchoolConnect</span></div>
            <div className="flex items-center gap-3">
            <p className="text-sm font-medium text-gray-800">{user.name}</p>
            <button onClick={handleLogout} className="px-3 py-1.5 text-xs text-gray-500 hover:bg-gray-100 rounded-lg">
                Đăng xuất
            </button>
            </div>
        </div>
        </header>
    );
}