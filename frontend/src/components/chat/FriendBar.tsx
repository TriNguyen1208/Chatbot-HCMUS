"use client";

import FriendMessageList from "./FriendMessageList";
import { SquarePen, Search } from "lucide-react";

const FriendBar = () => {
  return (
    <div className="w-full min-h-0  pt-2 flex flex-col gap-2 ">
      <div className="flex flex-col gap-1 px-3">
        <div className="flex flex-row justify-between items-center">
          <h3 className="uppercase text-txt-extra font-semibold text-xs">
            Messages
          </h3>
          <button
            type="button"
            className="bg-none hover:bg-input-surface p-2 rounded-lg hover:cursor-pointer"
            onClick={() => {}}
          >
            <SquarePen color="#003d9b" size={16} strokeWidth={2.5} />
          </button>
        </div>
        <div className="py-2 px-2 flex flex-row gap-2 justify-start items-center rounded-lg bg-input-surface">
          <Search color="#434654" size={14} strokeWidth={2} />
          <input
            className="placeholder:text-txt-extra text-sm outline-none"
            placeholder="Search friends..."
          />
        </div>
      </div>
      <FriendMessageList />
    </div>
  );
};

export default FriendBar;
