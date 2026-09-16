"use client";
import React from "react";
import { 
  Search, 
  UserPlus, 
  Settings, 
  Bell, 
  ShieldCheck, 
  UserMinus, 
  LogOut, 
  Trash2, 
  Ban 
} from "lucide-react";
import { Conversation } from "@/types";

export interface DangerActionsProps {
  activeConversation: Conversation;
  isAdmin: boolean;
  setIsSearchMode: (val: boolean) => void;
  setCreateGroupOpen: (val: boolean) => void;
  setShowEditGroupModal: (val: boolean) => void;
  setAssignAdminModalOpen: (val: boolean) => void;
  setKickModalOpen: (val: boolean) => void;
  handleLeaveGroup: () => void;
  setShowDisbandModal: (val: boolean) => void;
  isDisbanding: boolean;
  currentUserId?: string;
  handleUnblockUser: () => void;
  setShowBlockModal: (val: boolean) => void;
  isBlocking: boolean;
}

export const QuickActions: React.FC<{
  activeConversation: Conversation;
  isAdmin: boolean;
  setIsSearchMode: (val: boolean) => void;
  setCreateGroupOpen: (val: boolean) => void;
  setShowEditGroupModal: (val: boolean) => void;
}> = ({
  activeConversation,
  isAdmin,
  setIsSearchMode,
  setCreateGroupOpen,
  setShowEditGroupModal,
}) => {
  return (
    <div className="flex flex-row justify-center gap-4 py-4 border-b border-glass-border shrink-0">
      <button 
        className="flex flex-col items-center gap-1 group cursor-pointer"
        onClick={() => setIsSearchMode(true)}
      >
        <div className="p-3 bg-glass rounded-full group-hover:bg-brand-primary/10 group-hover:text-brand-primary transition-colors text-text-secondary cursor-pointer">
          <Search size={20} />
        </div>
        <span className="text-[11px] text-text-secondary group-hover:text-brand-primary cursor-pointer">Tìm kiếm</span>
      </button>

      {activeConversation.type === "utu" && (
        <button 
          className="flex flex-col items-center gap-1 group cursor-pointer"
          onClick={() => setCreateGroupOpen(true)}
        >
          <div className="p-3 bg-glass rounded-full group-hover:bg-brand-primary/10 group-hover:text-brand-primary transition-colors text-text-secondary cursor-pointer">
            <UserPlus size={20} />
          </div>
          <span className="text-[11px] text-text-secondary group-hover:text-brand-primary">Tạo nhóm</span>
        </button>
      )}

      {(activeConversation.type === "utu" || (activeConversation.type === "group" && isAdmin)) && (
        <button 
          className="flex flex-col items-center gap-1 group cursor-pointer"
          onClick={() => setShowEditGroupModal(true)}
        >
          <div className="p-3 bg-glass rounded-full group-hover:bg-brand-primary/10 group-hover:text-brand-primary transition-colors text-text-secondary cursor-pointer">
            <Settings size={20} />
          </div>
          <span className="text-[11px] text-text-secondary group-hover:text-brand-primary">Chỉnh sửa</span>
        </button>
      )}
      <button className="flex flex-col items-center gap-1 group cursor-pointer">
        <div className="p-3 bg-glass rounded-full group-hover:bg-brand-primary/10 group-hover:text-brand-primary transition-colors text-text-secondary">
          <Bell size={20} />
        </div>
        <span className="text-[11px] text-text-secondary group-hover:text-brand-primary">Thông báo</span>
      </button>
      {activeConversation.type === "group" && isAdmin && (
        <button 
          className="flex flex-col items-center gap-1 group cursor-pointer"
          onClick={() => setCreateGroupOpen(true)}
        >
          <div className="p-3 bg-glass rounded-full group-hover:bg-brand-primary/10 group-hover:text-brand-primary transition-colors text-text-secondary cursor-pointer">
            <UserPlus size={20} />
          </div>
          <span className="text-[11px] text-text-secondary group-hover:text-brand-primary">Thêm</span>
        </button>
      )}
    </div>
  );
};

export const DangerActions: React.FC<DangerActionsProps> = ({
  activeConversation,
  isAdmin,
  setAssignAdminModalOpen,
  setKickModalOpen,
  handleLeaveGroup,
  setShowDisbandModal,
  isDisbanding,
  currentUserId,
  handleUnblockUser,
  setShowBlockModal,
  isBlocking,
}) => {
  return (
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
        <>
          <button
            onClick={handleLeaveGroup}
            className="flex items-center gap-3 p-3 rounded-xl hover:bg-hover text-txt-primary transition-colors w-full cursor-pointer"
          >
            <LogOut size={20} />
            <span className="font-medium text-sm">Rời khỏi nhóm</span>
          </button>

          {isAdmin && (
            <button
              onClick={() => setShowDisbandModal(true)}
              disabled={isDisbanding}
              className="flex items-center gap-3 p-3 rounded-xl hover:bg-red-500/10 text-red-500 transition-colors w-full cursor-pointer disabled:opacity-50"
            >
              <Trash2 size={20} />
              <span className="font-medium text-sm">
                {isDisbanding ? "Đang xử lý..." : "Giải tán nhóm"}
              </span>
            </button>
          )}
        </>
      )}

      {/* 1-on-1 Block / Unblock */}
      {activeConversation.type === "utu" && (
        <>
          {activeConversation.block ? (
            activeConversation.block.block_by === currentUserId && (
              <button
                onClick={handleUnblockUser}
                disabled={isBlocking}
                className="flex items-center gap-3 p-3 rounded-xl hover:bg-brand-primary/10 text-brand-primary transition-colors w-full cursor-pointer disabled:opacity-50"
              >
                <ShieldCheck size={20} />
                <span className="font-medium text-sm">
                  {isBlocking ? "Đang xử lý..." : "Bỏ chặn người dùng"}
                </span>
              </button>
            )
          ) : (
            <button
              onClick={() => setShowBlockModal(true)}
              disabled={isBlocking}
              className="flex items-center gap-3 p-3 rounded-xl hover:bg-red-500/10 text-red-500 transition-colors w-full cursor-pointer disabled:opacity-50"
            >
              <Ban size={20} />
              <span className="font-medium text-sm">
                {isBlocking ? "Đang xử lý..." : "Chặn người dùng"}
              </span>
            </button>
          )}
        </>
      )}
    </div>
  );
};

export default DangerActions;
