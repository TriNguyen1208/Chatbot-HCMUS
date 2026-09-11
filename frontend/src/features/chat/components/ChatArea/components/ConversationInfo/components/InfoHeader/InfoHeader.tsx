"use client";
import React from "react";
import Image from "next/image";
import { X } from "lucide-react";
import { getRelativeTime } from "@/utils/formatTime";
import { Conversation } from "@/types";

export interface InfoHeaderProps {
  toggleInfoPanel: () => void;
  displayAvatar: string;
  displayName: string;
  activeConversation: Conversation;
  otherMember?: any;
  isOnline: boolean;
}

export const InfoHeader: React.FC<InfoHeaderProps> = ({
  toggleInfoPanel,
  displayAvatar,
  displayName,
  activeConversation,
  otherMember,
  isOnline,
}) => {
  return (
    <>
      {/* Top Header */}
      <div className="h-[64px] flex items-center justify-between px-4 border-b border-glass-border shrink-0">
        <h3 className="font-semibold text-lg text-text-primary">Thông tin hội thoại</h3>
        <button
          onClick={toggleInfoPanel}
          className="p-2 rounded-full hover:bg-glass cursor-pointer text-gray-500 hover:text-text-primary transition-colors"
        >
          <X size={20} />
        </button>
      </div>

      {/* Profile Section */}
      <div className="flex flex-col items-center py-6 px-4 border-b border-glass-border shrink-0">
        <div className="relative mb-3">
          <Image
            src={displayAvatar}
            alt="avatar"
            width={80}
            height={80}
            className="rounded-full object-cover size-20 shadow-md ring-4 ring-surface"
          />
          {activeConversation.type === "group" && isOnline && (
            <div className="absolute bottom-1 right-1 size-4 rounded-full bg-green-500 border-2 border-surface"></div>
          )}
        </div>
        <h2 className="text-xl font-bold text-text-primary text-center leading-tight">
          {displayName}
        </h2>
        {activeConversation.type === "utu" && otherMember && (
          <>
            <span 
              className="text-xs px-2.5 py-0.5 mt-1.5 rounded-full bg-brand-primary/10 text-brand-primary font-medium text-center max-w-[90%] truncate" 
              title={otherMember.role || "Khách"}
            >
              {otherMember.role || "Khách"}
            </span>
            <div className="text-sm mt-1">
              {isOnline ? (
                <span className="text-green-500 font-medium">Đang hoạt động</span>
              ) : (
                <span className="text-gray-500">
                  {otherMember.last_active
                    ? `Hoạt động ${getRelativeTime(otherMember.last_active)}`
                    : "Ngoại tuyến"}
                </span>
              )}
            </div>
          </>
        )}
      </div>
    </>
  );
};

export default InfoHeader;
