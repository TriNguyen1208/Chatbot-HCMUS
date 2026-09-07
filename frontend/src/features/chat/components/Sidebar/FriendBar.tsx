"use client";

import FriendMessageList from "./FriendMessageList";
import { SquarePen, Search, ArrowLeft } from "lucide-react";
import SearchUserModal from "../Modals/SearchUserModal";
import { useFriendBar } from "@/features/chat/hooks/useFriendBar";
import { useSearchStore } from "../../stores/searchStore";
import SearchSidebarContent from "./SearchSidebarContent";

const FriendBar = () => {
  const { isSearchOpen, setIsSearchOpen } = useFriendBar();
  const { isSearchMode, setSearchMode, searchQuery, setSearchQuery } = useSearchStore();

  return (
    <div className="w-full min-h-0 pt-2 flex flex-col gap-2 relative flex-1">
      <div className="flex flex-col gap-1 px-3">
        <div className="flex flex-row justify-between items-center">
          <h3 className="uppercase text-txt-extra font-semibold text-xs">
            Messages
          </h3>
          <button
            type="button"
            className="bg-none hover:bg-input-surface p-2 rounded-lg hover:cursor-pointer text-brand-primary"
            onClick={() => setIsSearchOpen(true)}
          >
            <SquarePen size={16} strokeWidth={2.5} />
          </button>
        </div>
        <div className="flex flex-row items-center gap-2">
          {isSearchMode && (
            <button 
                onClick={() => setSearchMode(false)}
                className="p-1.5 hover:bg-glass-panel rounded-full text-txt-primary shrink-0 transition-colors"
            >
                <ArrowLeft size={18} />
            </button>
          )}
          <div className="flex-1 py-2 px-2 flex flex-row gap-2 justify-start items-center rounded-lg bg-input-surface border border-glass-border shadow-inner">
            <Search className="text-ic-primary shrink-0" size={14} strokeWidth={2} />
            <input
              className="placeholder:text-txt-extra text-txt-primary text-sm outline-none bg-transparent w-full"
              placeholder="Search friends..."
              onFocus={() => setSearchMode(true)}
              value={isSearchMode ? searchQuery : ''}
              onChange={(e) => setSearchQuery(e.target.value)}
              readOnly={!isSearchMode}
            />
          </div>
        </div>
      </div>
      {isSearchMode ? <SearchSidebarContent /> : <FriendMessageList />}
      <SearchUserModal isOpen={isSearchOpen} onClose={() => setIsSearchOpen(false)} />
    </div>
  );
};

export default FriendBar;
