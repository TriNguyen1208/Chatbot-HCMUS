'use client';
import Image from "next/image";

import { Conversation } from "@/features/chat/types";
import { useChatCard } from "@/features/chat/hooks/useChatCard";
import { useUserStore } from "@/features/chat/stores/userStore";
import { useEffect } from "react";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";

interface ChatCardProps {
  conversation: Conversation;
  isActive: boolean;
  onClick: () => void;
}

const ChatCard = ({ conversation, isActive, onClick }: ChatCardProps) => {
  const { displayName, displayAvatar, timeDisplay, messagePreview, isOnline } = useChatCard(conversation);
  const { user } = useAuthStore();
  const typingUsersMap = useChatStore(state => state.typingUsers);

  const convId = conversation.id || (conversation as any).id;
  const currentTypingUsers = (convId ? (typingUsersMap[convId] || []) : []).filter(u => u.userId !== user?.id);
  const isTyping = currentTypingUsers.length > 0;

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
        {isOnline && (
          <div className="absolute bottom-0 right-0 size-3 rounded-full bg-green-500 border-2 border-surface-solid"></div>
        )}
      </div>
      <div className="flex-1 min-w-0 flex flex-col items-start justify-center gap-0.5">
        <div className="flex flex-row w-full justify-between items-center">
          <h3 className="font-semibold text-sm truncate pr-2 text-txt-primary">{displayName}</h3>
          <p className="text-txt-extra text-[11px] font-medium uppercase whitespace-nowrap">{timeDisplay}</p>
        </div>
        {isTyping ? (
          <p className="text-brand-primary text-xs w-full font-medium italic flex items-center gap-1">
            {currentTypingUsers.map(u => u.name).join(', ')} đang gõ
            <span className="flex gap-0.5 items-center">
                <span className="w-1 h-1 bg-brand-primary rounded-full animate-bounce [animation-delay:-0.3s]"></span>
                <span className="w-1 h-1 bg-brand-primary rounded-full animate-bounce [animation-delay:-0.15s]"></span>
                <span className="w-1 h-1 bg-brand-primary rounded-full animate-bounce"></span>
            </span>
          </p>
        ) : (
          <p className="line-clamp-1 text-txt-extra text-xs w-full truncate">{messagePreview}</p>
        )}
      </div>
    </div>
  );
}

export default ChatCard