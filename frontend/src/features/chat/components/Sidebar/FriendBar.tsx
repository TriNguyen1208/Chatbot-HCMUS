"use client";

import FriendMessageList from "./FriendMessageList";
import { SquarePen, Search } from "lucide-react";
import SearchUserModal from "../Modals/SearchUserModal";
import { useFriendBar } from "@/features/chat/hooks/useFriendBar";

const FriendBar = () => {
  const { isSearchOpen, setIsSearchOpen } = useFriendBar();

  return (
    <div className="w-full min-h-0  pt-2 flex flex-col gap-2 ">
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
        <div className="py-2 px-2 flex flex-row gap-2 justify-start items-center rounded-lg bg-input-surface border border-glass-border shadow-inner">
          <Search className="text-ic-primary" size={14} strokeWidth={2} />
          <input
            className="placeholder:text-txt-extra text-txt-primary text-sm outline-none bg-transparent"
            placeholder="Search friends..."
          />
        </div>
      </div>
      <FriendMessageList />
      <SearchUserModal isOpen={isSearchOpen} onClose={() => setIsSearchOpen(false)} />
    </div>
  );
};

export default FriendBar;
