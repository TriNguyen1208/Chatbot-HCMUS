"use client";
import { useChatHeader } from "@/features/chat/hooks/useChatHeader";
import {
  Phone,
  Video,
  Info,
} from "lucide-react";
import Image from "next/image";

import { getRelativeTime } from "@/utils/formatTime";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useModalStore } from "@/features/chat/stores/modalStore";

const ChatHeader = () => {
  const {
    activeConversation,
    displayName,
    displayAvatar,
    otherMember,
    isOnline,
  } = useChatHeader();

  const toggleInfoPanel = useChatStore(state => state.toggleInfoPanel);
  const openUserProfileModal = useModalStore(state => state.openUserProfileModal);

  if (!activeConversation) return null;

  const handleAvatarClick = () => {
    if (activeConversation.type === "utu" && otherMember) {
      openUserProfileModal(otherMember.id);
    } else {
      toggleInfoPanel();
    }
  };

  return (
    <>
      <div className="flex flex-row items-center justify-between w-full h-[64px] border-b border-glass-border px-5 bg-surface/80 backdrop-blur-xl z-10 shadow-sm transition-colors duration-300">
        <div className="flex flex-row items-center gap-3">
          <div 
            className={`relative ${activeConversation.type === 'utu' ? 'cursor-pointer hover:opacity-80 transition-opacity' : 'cursor-pointer'}`}
            onClick={handleAvatarClick}
          >
            <Image
              src={displayAvatar}
              alt="avatar"
              width={40}
              height={40}
              className="rounded-full object-cover shrink-0 size-10"
            />
            {activeConversation.type === "group" && isOnline && (
              <div className="absolute bottom-0 right-0 size-2.5 rounded-full bg-green-500 border-2 border-surface/80"></div>
            )}
          </div>
          <div className="flex flex-col">
            <h2 className="font-semibold text-lg">{displayName}</h2>
            {activeConversation.type === "utu" && otherMember && (
              <div className="flex items-center gap-1 text-xs">
                {isOnline ? (
                  <>
                    <div className="size-2 rounded-full bg-green-500"></div>
                    <span className="text-green-500">Đang hoạt động</span>
                  </>
                ) : (
                  <span className="text-gray-500">
                    {otherMember.last_active
                      ? `Hoạt động ${getRelativeTime(otherMember.last_active)}`
                      : "Ngoại tuyến"}
                  </span>
                )}
              </div>
            )}
            {activeConversation.type === "group" && (
              <span className="text-xs text-gray-500">
                {activeConversation.member_ids?.length || 0} thành viên
              </span>
            )}
          </div>
        </div>
        <div className="flex flex-row items-center gap-4 text-gray-500">
          {activeConversation.type !== 'self' && (
            <>
              <button className="hover:text-brand-primary cursor-pointer">
                <Phone size={20} />
              </button>
              <button className="hover:text-brand-primary cursor-pointer">
                <Video size={20} />
              </button>
            </>
          )}
          <button onClick={toggleInfoPanel} className="hover:text-brand-primary cursor-pointer">
            <Info size={20} />
          </button>

        </div>
      </div>
    </>
  );
};

export default ChatHeader;
