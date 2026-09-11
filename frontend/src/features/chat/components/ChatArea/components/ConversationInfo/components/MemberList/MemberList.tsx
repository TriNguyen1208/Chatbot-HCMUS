"use client";
import React from "react";
import Image from "next/image";
import { ChevronDown, ChevronRight } from "lucide-react";
import { DEFAULT_AVATAR } from "@/config/constants";
import { Conversation, User } from "@/types";

export interface MemberListProps {
  activeConversation: Conversation;
  isMembersExpanded: boolean;
  setIsMembersExpanded: (expanded: boolean) => void;
  users: Record<string, User>;
  currentUserId?: string;
  openUserProfileModal: (userId: string) => void;
}

export const MemberList: React.FC<MemberListProps> = ({
  activeConversation,
  isMembersExpanded,
  setIsMembersExpanded,
  users,
  currentUserId,
  openUserProfileModal,
}) => {
  if (activeConversation.type !== "group") return null;

  return (
    <div className="flex flex-col p-4 border-b border-glass-border shrink-0">
      <div 
        className="flex items-center justify-between mb-3 cursor-pointer group"
        onClick={() => setIsMembersExpanded(!isMembersExpanded)}
      >
        <h4 className="font-semibold text-sm text-text-secondary group-hover:text-text-primary transition-colors">
          Thành viên đoạn chat ({activeConversation.member_ids?.length || 0})
        </h4>
        {isMembersExpanded ? (
          <ChevronDown size={18} className="text-text-secondary transition-transform" />
        ) : (
          <ChevronRight size={18} className="text-text-secondary transition-transform" />
        )}
      </div>
      
      <div className={`flex flex-col gap-3 overflow-hidden transition-all duration-300 ${isMembersExpanded ? 'max-h-[300px] overflow-y-auto custom-scrollbar' : 'max-h-0'}`}>
        {activeConversation.member_ids?.map((memberId) => {
          const member = users[memberId];
          const isMe = memberId === currentUserId;
          const name = isMe ? "Bạn" : (member?.name || "Người dùng");
          const avatar = member?.avatar_url || DEFAULT_AVATAR;
          
          return (
            <div key={memberId} className="flex items-center gap-3">
              <div 
                className="relative cursor-pointer hover:opacity-80 transition-opacity"
                onClick={() => openUserProfileModal(memberId)}
              >
                <Image
                  src={avatar}
                  alt="avatar"
                  width={36}
                  height={36}
                  className="rounded-full object-cover size-9 shrink-0"
                />
                {!isMe && member?.is_online && (
                  <div className="absolute bottom-0 right-0 size-2.5 rounded-full bg-green-500 border-2 border-surface"></div>
                )}
              </div>
              <div className="flex flex-col min-w-0">
                <span className="text-sm font-medium text-text-primary truncate">
                  {name}
                </span>
                <div className="flex items-center gap-1.5 flex-wrap">
                  {activeConversation.admin_ids?.includes(memberId) && (
                    <span className="text-[10px] bg-brand-primary/10 text-brand-primary px-1.5 py-0.5 rounded w-fit">
                      Quản trị viên
                    </span>
                  )}
                  {member?.role && (
                    <span className="text-[10px] text-text-secondary truncate max-w-[150px]" title={member.role}>
                      {member.role}
                    </span>
                  )}
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};

export default MemberList;
