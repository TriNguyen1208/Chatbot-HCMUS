"use client";
import { useChatHeader } from "@/features/chat/hooks/useChatHeader";
import {
  Phone,
  Video,
  Info,
  MoreVertical,
  LogOut,
  UserMinus,
  User,
  UserPlus,
  ShieldCheck,
} from "lucide-react";
import Image from "next/image";
import CreateGroupModal from "../Modals/CreateGroupModal";
import KickMemberModal from "../Modals/KickMemberModal";
import AssignAdminModal from "../Modals/AssignAdminModal";

const ChatHeader = () => {
  const {
    activeConversation,
    showMenu,
    setShowMenu,
    isCreateGroupOpen,
    handleOpenCreateGroup,
    handleCloseCreateGroup,
    isKickModalOpen,
    handleOpenKickModal,
    handleCloseKickModal,
    isAssignAdminModalOpen,
    handleOpenAssignAdminModal,
    handleCloseAssignAdminModal,
    menuRef,
    displayName,
    displayAvatar,
    isAdmin,
    handleLeaveGroup,
  } = useChatHeader();

  if (!activeConversation) return null;

  return (
    <>
      <div className="flex flex-row items-center justify-between w-full h-[64px] border-b border-glass-border px-5 bg-surface/80 backdrop-blur-xl z-10 shadow-sm transition-colors duration-300">
        <div className="flex flex-row items-center gap-3">
          <Image
            src={displayAvatar}
            alt="avatar"
            width={40}
            height={40}
            className="rounded-full object-cover shrink-0 size-10"
          />
          <div className="flex flex-col">
            <h2 className="font-semibold text-lg">{displayName}</h2>
            <div className="flex items-center gap-1">
              <div className="size-2 rounded-full bg-green-500"></div>
              <span className="text-xs text-green-500">Online</span>
            </div>
          </div>
        </div>
        <div className="flex flex-row items-center gap-4 text-gray-500">
          <button className="hover:text-brand-primary cursor-pointer">
            <Phone size={20} />
          </button>
          <button className="hover:text-brand-primary cursor-pointer">
            <Video size={20} />
          </button>
          <button className="hover:text-brand-primary cursor-pointer">
            <Info size={20} />
          </button>

          <div className="relative" ref={menuRef}>
            <button
              onClick={() => setShowMenu(!showMenu)}
              className="hover:text-brand-primary cursor-pointer"
            >
              <MoreVertical size={20} />
            </button>
            {showMenu && (
              <div className="absolute right-0 mt-3 w-56 bg-surface-solid border border-glass-border shadow-2xl rounded-2xl py-2 flex flex-col text-sm z-50 animate-in fade-in zoom-in-95 duration-200 overflow-hidden">
                <button className="flex items-center gap-2.5 px-4 py-2.5 text-txt-primary hover:bg-hover text-left w-full transition-colors cursor-pointer">
                  <User size={16} /> Xem chi tiết
                </button>

                {activeConversation.type === "utu" && (
                  <button
                    onClick={handleOpenCreateGroup}
                    className="flex items-center gap-2.5 px-4 py-2 text-gray-700 hover:bg-gray-100 text-left w-full transition-colors cursor-pointer"
                  >
                    <UserPlus size={16} /> Tạo nhóm từ chat này
                  </button>
                )}

                {activeConversation.type === "group" && (
                  <>
                    <button
                      onClick={handleOpenCreateGroup}
                      className="flex items-center gap-2.5 px-4 py-2 text-gray-700 hover:bg-gray-100 text-left w-full transition-colors cursor-pointer"
                    >
                      <UserPlus size={16} /> Thêm thành viên
                    </button>

                    {isAdmin && (
                      <>
                        <button
                          onClick={handleOpenAssignAdminModal}
                          className="flex items-center gap-2.5 px-4 py-2 text-brand-primary hover:bg-blue-50 text-left w-full transition-colors cursor-pointer"
                        >
                          <ShieldCheck size={16} /> Cấp quyền Admin
                        </button>
                        <button
                          onClick={handleOpenKickModal}
                          className="flex items-center gap-2.5 px-4 py-2 text-orange-600 hover:bg-orange-50 text-left w-full transition-colors cursor-pointer"
                        >
                          <UserMinus size={16} /> Xóa thành viên
                        </button>
                      </>
                    )}

                    <div className="my-1 border-t border-gray-100" />

                    <button
                      onClick={handleLeaveGroup}
                      className="flex items-center gap-2.5 px-4 py-2 text-red-600 hover:bg-red-50 text-left w-full transition-colors cursor-pointer"
                    >
                      <LogOut size={16} /> Rời khỏi nhóm
                    </button>
                  </>
                )}
              </div>
            )}
          </div>
        </div>
      </div>

      <CreateGroupModal
        isOpen={isCreateGroupOpen}
        onClose={handleCloseCreateGroup}
      />

      <KickMemberModal
        isOpen={isKickModalOpen}
        onClose={handleCloseKickModal}
      />

      <AssignAdminModal
        isOpen={isAssignAdminModalOpen}
        onClose={handleCloseAssignAdminModal}
      />
    </>
  );
};

export default ChatHeader;
