"use client";

import { Users, User as UserIcon } from "lucide-react";
import { SearchResult } from "@/features/chat/api/search.api";
import { useSearchItem } from "./useSearchItem";

const SearchItem = ({ item }: { item: SearchResult }) => {
  const { displayAvatar, displayName, handleClick } = useSearchItem(item);

  if (item.search_type === 'user') {
    return (
      <div 
        onClick={handleClick}
        className="flex items-center gap-3 p-2 rounded-lg hover:bg-glass-panel cursor-pointer transition-colors group"
      >
        <div className="relative size-10 shrink-0 rounded-full bg-surface-solid border border-glass-border flex items-center justify-center overflow-hidden">
          {item.avatar_url ? (
            <img src={item.avatar_url} alt={item.name} className="w-full h-full object-cover" />
          ) : (
            <UserIcon size={20} className="text-brand-primary" />
          )}
        </div>
        <div className="flex flex-col flex-1 min-w-0">
          <span className="text-sm font-semibold text-txt-primary truncate group-hover:text-brand-primary transition-colors">
            {item.name}
          </span>
          <span className="text-xs text-txt-extra truncate">User</span>
        </div>
      </div>
    );
  }

  if (item.search_type === 'conversation') {
    return (
      <div 
        onClick={handleClick}
        className="flex items-center gap-3 p-2 rounded-lg hover:bg-glass-panel cursor-pointer transition-colors group"
      >
        <div className="relative size-10 shrink-0 rounded-full bg-surface-solid border border-glass-border flex items-center justify-center overflow-hidden">
          {displayAvatar ? (
            <img src={displayAvatar} alt={displayName} className="w-full h-full object-cover" />
          ) : (
            <Users size={20} className="text-brand-primary" />
          )}
        </div>
        <div className="flex flex-col flex-1 min-w-0">
          <span className="text-sm font-semibold text-txt-primary truncate group-hover:text-brand-primary transition-colors">
            {displayName}
          </span>
          <span className="text-xs text-txt-extra truncate">Conversation</span>
        </div>
      </div>
    );
  }

  if (item.search_type === 'message' && item.sender && item.conversation) {
    return (
      <div 
        onClick={handleClick}
        className="flex items-start gap-3 p-2 rounded-lg hover:bg-glass-panel cursor-pointer transition-colors group"
      >
        <div className="mt-1">
          <div className="relative size-10 shrink-0 rounded-full bg-surface-solid border border-glass-border flex items-center justify-center overflow-hidden">
            {item.sender.avatar_url ? (
              <img src={item.sender.avatar_url} alt={item.sender.name} className="w-full h-full object-cover" />
            ) : (
              <UserIcon size={20} className="text-brand-primary" />
            )}
          </div>
        </div>
        <div className="flex flex-col flex-1 min-w-0 gap-0.5">
          <div className="flex items-center justify-between">
            <span className="text-sm font-semibold text-txt-primary truncate">
              {item.sender.name}
            </span>
          </div>
          <p className="text-sm text-txt-primary line-clamp-2">
            {item.content || (item as any).text}
          </p>
          <div className="flex items-center gap-1 mt-1 text-xs text-brand-primary/80 truncate">
            <span className="font-medium truncate">{item.conversation.name}</span>
          </div>
        </div>
      </div>
    );
  }

  return null;
};

export default SearchItem;
