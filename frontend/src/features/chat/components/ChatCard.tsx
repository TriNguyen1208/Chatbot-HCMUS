'use client';
import Image from "next/image";

import { Conversation } from "@/features/chat/types";
import { useChatCard } from "@/features/chat/hooks/useChatCard";

interface ChatCardProps {
  conversation: Conversation;
  isActive: boolean;
  onClick: () => void;
}

const ChatCard = ({ conversation, isActive, onClick }: ChatCardProps) => {
  const { displayName, displayAvatar, timeDisplay, messagePreview } = useChatCard(conversation);

  return (
    <div 
      onClick={onClick}
      className={`group flex flex-row gap-3 justify-start items-center w-full hover:bg-hover hover:shadow-sm hover:-translate-y-[1px] hover:cursor-pointer px-3 py-3 rounded-xl transition-all duration-200 border ${isActive ? 'bg-hover shadow-sm border-border-primary/50' : 'border-transparent'}`}
    >
      <div className="relative">
        <Image
          src={displayAvatar}
          alt="avatar"
          width={44}
          height={44}
          className="rounded-full object-cover shrink-0 size-11 ring-2 ring-transparent group-hover:ring-brand-primary/20 transition-all duration-300"
        />
      </div>
      <div className="flex-1 min-w-0 flex flex-col items-start justify-center gap-0.5">
        <div className="flex flex-row w-full justify-between items-center">
          <h3 className="font-semibold text-sm truncate pr-2 text-txt-primary">{displayName}</h3>
          <p className="text-txt-extra text-[11px] font-medium uppercase whitespace-nowrap">{timeDisplay}</p>
        </div>
        <p className="line-clamp-1 text-txt-extra text-xs w-full truncate">{messagePreview}</p>
      </div>
    </div>
  );
}

export default ChatCard