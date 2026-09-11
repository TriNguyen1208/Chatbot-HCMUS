"use client";

import { useState, useMemo } from "react";
import { useModalStore } from "@/features/chat/stores/modalStore";
import { useConversationsQuery } from "@/features/chat/hooks/useChatQueries";
import { messageApi } from "@/features/chat/api/message.api";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useUserStore } from "@/features/chat/stores/userStore";

export const useForwardModal = () => {
  const { isForwardModalOpen, closeForwardModal, forwardMessageData } = useModalStore();
  const { data, isLoading } = useConversationsQuery();

  const [searchQuery, setSearchQuery] = useState("");
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [isSending, setIsSending] = useState(false);

  const { user } = useAuthStore();
  const { users } = useUserStore();

  const allConversations = useMemo(() => {
    return data?.pages.flatMap((page) => page) || [];
  }, [data]);

  const filteredConversations = useMemo(() => {
    if (!searchQuery.trim()) return allConversations;

    return allConversations.filter((conv) => {
      const otherMemberId = conv.member_ids?.find((m: string) => m !== user?.id) as string;
      const otherMember = users[otherMemberId];
      const displayName = conv.name || otherMember?.name || "Người dùng";

      return displayName.toLowerCase().includes(searchQuery.toLowerCase());
    });
  }, [allConversations, searchQuery, user, users]);

  const toggleSelect = (id: string) => {
    setSelectedIds((prev) =>
      prev.includes(id) ? prev.filter((item) => item !== id) : [...prev, id]
    );
  };

  const handleSend = async () => {
    if (selectedIds.length === 0 || !forwardMessageData) return;
    setIsSending(true);

    try {
      const payload: any = {
        type: forwardMessageData.type,
      };

      if (forwardMessageData.content) payload.content = forwardMessageData.content;
      if (forwardMessageData.image) payload.image = forwardMessageData.image;
      if (forwardMessageData.video) payload.video = forwardMessageData.video;

      await Promise.all(
        selectedIds.map((convId) =>
          messageApi.sendMessage({
            ...payload,
            conversation_id: convId,
          })
        )
      );

      closeForwardModal();
      setSelectedIds([]);
      setSearchQuery("");
    } catch (error) {
      console.error("Failed to forward message", error);
    } finally {
      setIsSending(false);
    }
  };

  return {
    isForwardModalOpen,
    closeForwardModal,
    forwardMessageData,
    isLoading,
    searchQuery,
    setSearchQuery,
    selectedIds,
    isSending,
    filteredConversations,
    toggleSelect,
    handleSend,
  };
};
