'use client';
import Image from "next/image";
import { Conversation } from "@/types";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useChatCard } from "./useChatCard";

export interface ChatCardProps {
  conversation: Conversation;
  isActive: boolean;
  onClick: () => void;
}

export const ChatCard = ({ conversation, isActive, onClick }: ChatCardProps) => {
  const { displayName, displayAvatar, timeDisplay, messagePreview, isOnline, isUnread } = useChatCard(conversation);
  const { user } = useAuthStore();
  const typingUsersMap = useChatStore(state => state.typingUsers);

  const convId = conversation.id;
  const currentTypingUsers = (convId ? (typingUsersMap[convId] || []) : []).filter(u => u.userId !== user?.id);
  const isTyping = currentTypingUsers.length > 0;
  const hasUnread = isUnread && !isActive;

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
          className="rounded-full object-cover shrink-0 size-11 shadow-sm border border-glass-border"
        />
        {isOnline && (
          <div className="absolute bottom-0 right-0 size-3 rounded-full bg-green-500 border-2 border-surface"></div>
        )}
      </div>
      <div className="flex-1 min-w-0 flex flex-col items-start justify-center gap-0.5">
        <div className="flex flex-row w-full justify-between items-center">
          <h3 className={`text-sm truncate pr-2 text-txt-primary ${hasUnread ? 'font-bold' : 'font-semibold'}`}>
            {displayName}
          </h3>
          <div className="flex items-center gap-1.5 shrink-0">
            <p className={`text-[11px] uppercase whitespace-nowrap ${hasUnread ? 'font-semibold text-brand-primary' : 'text-txt-extra font-medium'}`}>
              {timeDisplay}
            </p>
            {hasUnread && (
              <span className="size-2 rounded-full bg-brand-primary shrink-0 animate-pulse" />
            )}
          </div>
        </div>
        {isTyping ? (
          <p className="text-brand-primary text-xs w-full font-medium italic flex items-center gap-1">
            <span className="truncate">
              {currentTypingUsers.length === 1 
                ? `${currentTypingUsers[0].name} đang soạn tin...`
                : `${currentTypingUsers.length} người đang soạn tin...`}
            </span>
          </p>
        ) : (
          <p className={`line-clamp-1 text-xs w-full truncate ${hasUnread ? 'font-semibold text-txt-primary' : 'text-txt-extra font-normal'}`}>
            {messagePreview}
          </p>
        )}
      </div>
    </div>
  );
};

export default ChatCard;
