"use client";

import { useSidebarProfile } from "@/features/chat/hooks/useSidebarProfile";
import { LogOut } from "lucide-react";

const SidebarProfile = () => {
  const { user, handleLogout } = useSidebarProfile();

  return (
    <div className="flex flex-row items-center justify-between w-full h-[64px] border-t border-glass-border px-4 bg-surface/50 backdrop-blur-md mt-auto transition-colors duration-300">
      <div className="flex flex-row items-center gap-3 w-full">
        <div className="flex items-center justify-center size-9 rounded-full bg-gradient-primary shadow-sm border border-glass-border">
          <span className="font-bold text-white text-sm uppercase">
            {user?.name?.[0] || 'U'}
          </span>
        </div>
        <div className="flex flex-col flex-1 min-w-0">
          <span className="text-sm font-semibold truncate text-txt-primary">{user?.name || 'User'}</span>
          <span className="text-[11px] text-txt-extra font-medium truncate">Active Researcher</span>
        </div>
        <button 
          onClick={handleLogout}
          className="p-2 hover:bg-hover rounded-xl text-ic-primary hover:text-red-500 transition-colors"
          title="Logout"
        >
          <LogOut size={18} />
        </button>
      </div>
    </div>
  );
};

export default SidebarProfile;
