"use client";

import { useState, useEffect } from "react";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { conversationApi } from "@/features/chat/api/conversation.api";
import { useUserStore } from "@/features/chat/stores/userStore";

export const useAssignAdminModal = (isOpen: boolean, onClose: () => void) => {
  const { activeConversation, setActiveConversation } = useChatStore();
  const { user } = useAuthStore();
  const { users, requestUser } = useUserStore();

  const [selectedAdminIds, setSelectedAdminIds] = useState<string[]>([]);
  const [searchQuery, setSearchQuery] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [errorMsg, setErrorMsg] = useState("");

  useEffect(() => {
    if (isOpen) {
      setSelectedAdminIds([]);
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

  const currentAdminIds = new Set(activeConversation?.admin_ids || []);

  const nonAdminMembers = (activeConversation?.member_ids || []).filter((m: string) => {
    return m && !currentAdminIds.has(m);
  });

  const filteredMembers = nonAdminMembers.filter((mId) => {
    if (!searchQuery.trim()) return true;
    const q = searchQuery.toLowerCase();
    const u = users[mId as string];
    return (
      u?.name?.toLowerCase().includes(q) ||
      u?.email?.toLowerCase().includes(q)
    );
  });

  const toggleSelectUser = (userId: string) => {
    setErrorMsg("");
    setSelectedAdminIds((prev) =>
      prev.includes(userId)
        ? prev.filter((id) => id !== userId)
        : [...prev, userId]
    );
  };

  const handleAssignAdmins = async () => {
    if (selectedAdminIds.length === 0 || isSubmitting || !activeConversation) return;

    try {
      setIsSubmitting(true);
      setErrorMsg("");

      const convId = activeConversation.id as string;
      await conversationApi.assignAdmins(convId, selectedAdminIds);

      const newlyPromoted = (activeConversation.member_ids || []).filter((m: string) =>
        selectedAdminIds.includes(m)
      );

      setActiveConversation({
        ...activeConversation,
        admin_ids: [...(activeConversation.admin_ids || []), ...newlyPromoted],
      });

      onClose();
    } catch (error: unknown) {
      console.error("Lỗi cấp quyền Admin:", error);
      setErrorMsg(
        (error as Error)?.message || "Không thể cấp quyền Admin"
      );
    } finally {
      setIsSubmitting(false);
    }
  };

  return {
    activeConversation,
    users,
    selectedAdminIds,
    searchQuery,
    setSearchQuery,
    isSubmitting,
    errorMsg,
    filteredMembers,
    toggleSelectUser,
    handleAssignAdmins,
  };
};
