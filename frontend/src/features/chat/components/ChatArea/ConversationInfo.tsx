"use client";
import React, { useState, useRef, useCallback, useEffect } from "react";
import Image from "next/image";
import { X, Bell, Search, UserPlus, LogOut, ChevronRight, ChevronDown, ShieldCheck, UserMinus, Settings } from "lucide-react";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useChatHeader } from "@/features/chat/hooks/useChatHeader";
import { useUserStore } from "@/features/chat/stores/userStore";
import { getRelativeTime } from "@/utils/formatTime";
import { DEFAULT_AVATAR } from "@/utils/constants";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useModalStore } from "@/features/chat/stores/modalStore";

const MIN_WIDTH = 250;
const MAX_WIDTH = 500;
const DEFAULT_WIDTH = 320;

const ConversationInfo = () => {
  const showInfoPanel = useChatStore((state) => state.showInfoPanel);
  const toggleInfoPanel = useChatStore((state) => state.toggleInfoPanel);
  const { user } = useAuthStore();
  const users = useUserStore((state) => state.users);

  const {
    setCreateGroupOpen,
    setAssignAdminModalOpen,
    setKickModalOpen,
    openUserProfileModal
  } = useModalStore();

  const {
    activeConversation,
    displayName,
    displayAvatar,
    otherMember,
    isOnline,
    handleLeaveGroup,
    isAdmin,
  } = useChatHeader();

  const [panelWidth, setPanelWidth] = useState(DEFAULT_WIDTH);
  const isResizing = useRef(false);
  
  const [isMembersExpanded, setIsMembersExpanded] = useState(true);

  // Resize logic
  const handleMouseDown = (e: React.MouseEvent) => {
    isResizing.current = true;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none'; // Prevent text selection while dragging
  };

  const handleMouseMove = useCallback((e: MouseEvent) => {
    if (!isResizing.current) return;
    
    // Calculate new width from right edge of screen
    const newWidth = window.innerWidth - e.clientX;
    
    if (newWidth >= MIN_WIDTH && newWidth <= MAX_WIDTH) {
      setPanelWidth(newWidth);
    } else if (newWidth < MIN_WIDTH) {
      setPanelWidth(MIN_WIDTH);
    } else if (newWidth > MAX_WIDTH) {
      setPanelWidth(MAX_WIDTH);
    }
  }, []);

  const handleMouseUp = useCallback(() => {
    isResizing.current = false;
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
  }, []);

  useEffect(() => {
    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);
    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [handleMouseMove, handleMouseUp]);

  if (!showInfoPanel || !activeConversation) return null;

  return (
    <div 
      className="relative flex flex-col h-full bg-surface/50 backdrop-blur-xl shrink-0 transition-none border-l border-glass-border overflow-hidden"
      style={{ width: `${panelWidth}px` }}
    >
      {/* Resizer Handle */}
      <div 
        className="absolute left-0 top-0 bottom-0 w-1.5 cursor-col-resize hover:bg-brand-primary/50 transition-colors z-10"
        onMouseDown={handleMouseDown}
      />

      <div className="flex flex-col w-full h-full overflow-y-auto custom-scrollbar">
        {/* Header */}
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
          )}
        </div>

        {/* Quick Actions */}
        <div className="flex flex-row justify-center gap-4 py-4 border-b border-glass-border shrink-0">
          <button className="flex flex-col items-center gap-1 group">
            <div className="p-3 bg-glass rounded-full group-hover:bg-brand-primary/10 group-hover:text-brand-primary transition-colors text-text-secondary">
              <Search size={20} />
            </div>
            <span className="text-[11px] text-text-secondary group-hover:text-brand-primary">Tìm kiếm</span>
          </button>

          {activeConversation.type === "utu" && (
            <button 
              className="flex flex-col items-center gap-1 group"
              onClick={() => setCreateGroupOpen(true)}
            >
              <div className="p-3 bg-glass rounded-full group-hover:bg-brand-primary/10 group-hover:text-brand-primary transition-colors text-text-secondary cursor-pointer">
                <UserPlus size={20} />
              </div>
              <span className="text-[11px] text-text-secondary group-hover:text-brand-primary">Tạo nhóm</span>
            </button>
          )}

          {activeConversation.type === "group" && isAdmin && (
            <button className="flex flex-col items-center gap-1 group">
              <div className="p-3 bg-glass rounded-full group-hover:bg-brand-primary/10 group-hover:text-brand-primary transition-colors text-text-secondary">
                <Settings size={20} />
              </div>
              <span className="text-[11px] text-text-secondary group-hover:text-brand-primary">Chỉnh sửa</span>
            </button>
          )}
          <button className="flex flex-col items-center gap-1 group">
            <div className="p-3 bg-glass rounded-full group-hover:bg-brand-primary/10 group-hover:text-brand-primary transition-colors text-text-secondary">
              <Bell size={20} />
            </div>
            <span className="text-[11px] text-text-secondary group-hover:text-brand-primary">Thông báo</span>
          </button>
          {activeConversation.type === "group" && (
            <button 
              className="flex flex-col items-center gap-1 group"
              onClick={() => setCreateGroupOpen(true)}
            >
              <div className="p-3 bg-glass rounded-full group-hover:bg-brand-primary/10 group-hover:text-brand-primary transition-colors text-text-secondary cursor-pointer">
                <UserPlus size={20} />
              </div>
              <span className="text-[11px] text-text-secondary group-hover:text-brand-primary">Thêm</span>
            </button>
          )}
        </div>

        {/* Group Members Section */}
        {activeConversation.type === "group" && (
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
                const isMe = memberId === user?.id;
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
                      {activeConversation.admin_ids?.includes(memberId) && (
                        <span className="text-[10px] bg-brand-primary/10 text-brand-primary px-1.5 py-0.5 rounded w-fit">
                          Quản trị viên
                        </span>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* Settings / Danger Zone */}
        <div className="flex flex-col mt-auto p-4 gap-2 shrink-0">
          
          {activeConversation.type === "group" && isAdmin && (
            <>
              <button
                onClick={() => setAssignAdminModalOpen(true)}
                className="flex items-center gap-3 p-3 rounded-xl hover:bg-blue-500/10 text-blue-500 transition-colors w-full cursor-pointer"
              >
                <ShieldCheck size={20} />
                <span className="font-medium text-sm">Cấp quyền Admin</span>
              </button>
              <button
                onClick={() => setKickModalOpen(true)}
                className="flex items-center gap-3 p-3 rounded-xl hover:bg-orange-500/10 text-orange-500 transition-colors w-full cursor-pointer"
              >
                <UserMinus size={20} />
                <span className="font-medium text-sm">Xóa thành viên</span>
              </button>
            </>
          )}

          {activeConversation.type === "group" && (
            <button
              onClick={handleLeaveGroup}
              className="flex items-center gap-3 p-3 rounded-xl hover:bg-red-500/10 text-red-500 transition-colors w-full cursor-pointer"
            >
              <LogOut size={20} />
              <span className="font-medium text-sm">Rời khỏi nhóm</span>
            </button>
          )}
        </div>
      </div>
    </div>
  );
};

export default ConversationInfo;
