"use client";
import { useState, useEffect } from "react";
import { X, Search, UserMinus, Check, AlertTriangle } from "lucide-react";
import Image from "next/image";
import { useQueryClient } from "@tanstack/react-query";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { DEFAULT_AVATAR } from "@/utils/constants";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { conversationApi } from "@/features/chat/api/conversation.api";
import { User } from "@/features/chat/types";

interface KickMemberModalProps {
  isOpen: boolean;
  onClose: () => void;
}

export default function KickMemberModal({
  isOpen,
  onClose,
}: KickMemberModalProps) {
  const { activeConversation, setActiveConversation } = useChatStore();
  const { user } = useAuthStore();
  const queryClient = useQueryClient();

  const [selectedUserIds, setSelectedUserIds] = useState<string[]>([]);
  const [searchQuery, setSearchQuery] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [errorMsg, setErrorMsg] = useState("");

  useEffect(() => {
    if (isOpen) {
      setSelectedUserIds([]);
      setSearchQuery("");
      setIsSubmitting(false);
      setErrorMsg("");
    }
  }, [isOpen]);

  if (!isOpen || !activeConversation) return null;

  // Filter members excluding self and admins
  const adminIds = new Set(
    (activeConversation.admins || []).map((a) => a.id || (a as any)._id)
  );

  const kickableMembers = (activeConversation.members || []).filter((m) => {
    const mId = m.id || (m as any)._id;
    return mId !== user?.id && !adminIds.has(mId);
  });

  const filteredMembers = kickableMembers.filter((m) => {
    if (!searchQuery.trim()) return true;
    const q = searchQuery.toLowerCase();
    return (
      m.name?.toLowerCase().includes(q) ||
      m.email?.toLowerCase().includes(q)
    );
  });

  const totalMembers = activeConversation.members?.length || 0;
  const remainingCount = totalMembers - selectedUserIds.length;
  const isMinMembersViolation = remainingCount < 2;

  const toggleSelectUser = (userId: string) => {
    setErrorMsg("");
    setSelectedUserIds((prev) =>
      prev.includes(userId)
        ? prev.filter((id) => id !== userId)
        : [...prev, userId]
    );
  };

  const handleKickMembers = async () => {
    if (selectedUserIds.length === 0 || isSubmitting || isMinMembersViolation)
      return;

    try {
      setIsSubmitting(true);
      setErrorMsg("");

      const convId = activeConversation._id || (activeConversation as any).id;
      await conversationApi.removeMembers(convId, selectedUserIds);

      // Invalidate queries to refresh sidebar and conversation details
      queryClient.invalidateQueries({ queryKey: ["conversations"] });

      // Update activeConversation members locally
      const updatedMembers = (activeConversation.members || []).filter(
        (m) => !selectedUserIds.includes(m.id || (m as any)._id)
      );

      setActiveConversation({
        ...activeConversation,
        members: updatedMembers,
      });

      onClose();
    } catch (error: any) {
      console.error("Lỗi xóa thành viên:", error);
      setErrorMsg(
        error?.response?.data?.message || "Không thể xóa thành viên khỏi nhóm"
      );
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-md">
      <div className="bg-surface/90 backdrop-blur-2xl rounded-[2rem] w-[460px] max-h-[85vh] flex flex-col overflow-hidden shadow-2xl border border-glass-border animate-in fade-in zoom-in-95 duration-200">
        {/* Header */}
        <div className="flex items-center justify-between p-5 border-b border-glass-border bg-red-500/10">
          <div className="flex items-center gap-3 text-red-600 dark:text-red-400 font-semibold text-lg">
            <UserMinus className="size-5" />
            <span>Xóa thành viên khỏi nhóm</span>
          </div>
          <button
            onClick={onClose}
            className="p-1 hover:bg-gray-200/60 text-gray-500 rounded-md transition-colors"
          >
            <X size={20} />
          </button>
        </div>

        <div className="p-4 flex flex-col gap-3 overflow-y-auto flex-1">
          {/* Member count info & constraint badge */}
          <div
            className={`p-3 rounded-lg border text-xs flex items-center justify-between ${
              isMinMembersViolation
                ? "bg-red-50 border-red-200 text-red-700"
                : "bg-gray-50 border-gray-200 text-gray-700"
            }`}
          >
            <span>
              Tổng số thành viên: <strong>{totalMembers}</strong>
            </span>
            <span>
              Còn lại sau xóa:{" "}
              <strong
                className={isMinMembersViolation ? "text-red-600 font-bold" : ""}
              >
                {remainingCount}
              </strong>
            </span>
          </div>

          {isMinMembersViolation && (
            <div className="flex items-center gap-1.5 p-2.5 bg-red-100/70 border border-red-200 rounded-lg text-xs text-red-700 font-medium">
              <AlertTriangle className="size-4 shrink-0 text-red-600" />
              <span>Nhóm phải duy trì tối thiểu 2 thành viên!</span>
            </div>
          )}

          {errorMsg && (
            <div className="p-2.5 bg-red-50 border border-red-200 rounded-lg text-xs text-red-600 font-medium">
              {errorMsg}
            </div>
          )}

          {/* Search bar */}
          <div className="relative">
            <Search
              size={16}
              className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400"
            />
            <input
              type="text"
              placeholder="Tìm kiếm thành viên cần xóa..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full pl-9 pr-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-red-500/20 focus:border-red-500 transition-all"
            />
          </div>

          {/* Members List */}
          <div className="flex flex-col gap-1 max-h-[240px] overflow-y-auto pr-1">
            {filteredMembers.length === 0 ? (
              <p className="text-center text-xs text-gray-400 py-6">
                Không tìm thấy thành viên phù hợp
              </p>
            ) : (
              filteredMembers.map((m) => {
                const memberId = (m.id || (m as any)._id) as string;
                const isSelected = selectedUserIds.includes(memberId);

                return (
                  <div
                    key={memberId}
                    onClick={() => toggleSelectUser(memberId)}
                    className={`flex items-center justify-between p-2.5 rounded-lg cursor-pointer transition-colors ${
                      isSelected
                        ? "bg-red-50/70 border border-red-200"
                        : "hover:bg-gray-50 border border-transparent"
                    }`}
                  >
                    <div className="flex items-center gap-2.5 min-w-0">
                      <Image
                        src={m.avatar_url || DEFAULT_AVATAR}
                        alt="avatar"
                        width={32}
                        height={32}
                        className="rounded-full object-cover size-8 shrink-0"
                      />
                      <div className="flex flex-col min-w-0">
                        <span className="text-sm font-medium text-gray-800 truncate">
                          {m.name}
                        </span>
                        <span className="text-xs text-gray-400 truncate">
                          {m.email}
                        </span>
                      </div>
                    </div>

                    <div
                      className={`size-5 rounded-md flex items-center justify-center border transition-all ${
                        isSelected
                          ? "bg-red-600 border-red-600 text-white"
                          : "border-gray-300 bg-white"
                      }`}
                    >
                      {isSelected && <Check size={13} strokeWidth={3} />}
                    </div>
                  </div>
                );
              })
            )}
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 p-4 border-t border-glass-border bg-surface-solid/50">
          <button
            onClick={onClose}
            className="px-5 py-2.5 text-sm font-medium text-txt-primary hover:bg-hover rounded-xl transition-colors cursor-pointer"
          >
            Hủy
          </button>
          <button
            onClick={handleKickMembers}
            disabled={
              selectedUserIds.length === 0 ||
              isSubmitting ||
              isMinMembersViolation
            }
            className="px-5 py-2.5 text-sm font-medium text-white bg-gradient-to-r from-red-500 to-red-600 hover:from-red-600 hover:to-red-700 rounded-xl disabled:opacity-50 disabled:shadow-none disabled:cursor-not-allowed transition-all shadow-md hover:shadow-lg hover:-translate-y-0.5 cursor-pointer"
          >
            {isSubmitting
              ? "Đang xóa..."
              : `Xóa (${selectedUserIds.length}) thành viên`}
          </button>
        </div>
      </div>
    </div>
  );
}
