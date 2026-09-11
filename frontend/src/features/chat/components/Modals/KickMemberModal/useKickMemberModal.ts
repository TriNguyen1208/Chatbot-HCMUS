"use client";

import { useState, useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { conversationApi } from "@/features/chat/api/conversation.api";
import { useUserStore } from "@/features/chat/stores/userStore";

export const useKickMemberModal = (isOpen: boolean, onClose: () => void) => {
  const { activeConversation, setActiveConversation } = useChatStore();
  const { user } = useAuthStore();
  const { users, requestUser } = useUserStore();
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

  useEffect(() => {
    if (isOpen && activeConversation?.member_ids) {
      activeConversation.member_ids.forEach((mId: string) => {
        if (!users[mId]) {
          requestUser(mId);
        }
      });
    }
  }, [isOpen, activeConversation, users, requestUser]);

  const adminIds = new Set(activeConversation?.admin_ids || []);

  const kickableMembers = (activeConversation?.member_ids || []).filter((m: string) => {
    return m !== user?.id && !adminIds.has(m);
  });

  const filteredMembers = kickableMembers.filter((mId) => {
    if (!searchQuery.trim()) return true;
    const q = searchQuery.toLowerCase();
    const u = users[mId as string];
    return (
      u?.name?.toLowerCase().includes(q) ||
      u?.email?.toLowerCase().includes(q)
    );
  });

  const totalMembers = activeConversation?.member_ids?.length || 0;
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
    if (selectedUserIds.length === 0 || isSubmitting || isMinMembersViolation || !activeConversation)
      return;

    try {
      setIsSubmitting(true);
      setErrorMsg("");

      const convId = activeConversation.id as string;
      await conversationApi.removeMembers(convId, selectedUserIds);

      queryClient.invalidateQueries({ queryKey: ["conversations"] });

      const updatedMembers = (activeConversation.member_ids || []).filter(
        (m) => !selectedUserIds.includes(m)
      );

      setActiveConversation({
        ...activeConversation,
        member_ids: updatedMembers,
      });

      onClose();
    } catch (error: unknown) {
      console.error("Lỗi xóa thành viên:", error);
      setErrorMsg(
        (error as Error)?.message || "Không thể xóa thành viên khỏi nhóm"
      );
    } finally {
      setIsSubmitting(false);
    }
  };

  return {
    activeConversation,
    users,
    selectedUserIds,
    searchQuery,
    setSearchQuery,
    isSubmitting,
    errorMsg,
    filteredMembers,
    kickableMembers,
    isMinMembersViolation,
    toggleSelectUser,
    handleKickMembers,
  };
};
