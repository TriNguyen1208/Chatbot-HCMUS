"use client";

import { useEffect } from "react";
import { useRouter, usePathname } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import { SearchResult } from "@/features/chat/api/search.api";
import { conversationApi } from "@/features/chat/api/conversation.api";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useSearchStore } from "@/features/chat/stores/searchStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useUserStore } from "@/features/chat/stores/userStore";
import { Conversation } from "@/types";
import { DEFAULT_AVATAR } from "@/config/constants";
import { chatCache } from "@/features/chat/utils/chat-cache.util";

export const useSearchItem = (item: SearchResult) => {
  const { setActiveConversation } = useChatStore();
  const { setTargetMessageId, setSearchMode } = useSearchStore();
  const { user } = useAuthStore();
  const { users, requestUser } = useUserStore();
  const router = useRouter();
  const pathname = usePathname();
  const basePath = pathname.startsWith('/direct-chat') || pathname.startsWith('/group-chat') || pathname.startsWith('/chat')
    ? pathname
    : '/chat';

  // If item is a conversation, resolve its full data and avatar_url
  const isConversation = item.search_type === 'conversation';
  const { data: convData } = useQuery<Conversation | null>({
    queryKey: ['conversation', item.id],
    queryFn: async () => {
      const res = await conversationApi.getConversationById(item.id);
      if (res) chatCache.setConversation(res);
      return (res as any).data || res;
    },
    enabled: isConversation && !!item.id,
    initialData: () => {
      if (!isConversation || !item.id) return undefined;
      return chatCache.getConversation(item.id);
    },
    staleTime: 1000 * 60 * 5,
  });

  // In case it's a 1-1 conversation and needs otherMember's avatar
  const otherMemberId = convData?.member_ids?.find((m: string) => m !== user?.id) as string;
  const otherMember = users[otherMemberId];

  useEffect(() => {
    if (convData?.type === 'utu' && otherMemberId && !otherMember) {
      requestUser(otherMemberId);
    }
  }, [convData?.type, otherMemberId, otherMember, requestUser]);

  const displayAvatar = item.avatar_url 
    || convData?.avatar_url 
    || (convData?.type === 'self' ? (user?.avatar_url || DEFAULT_AVATAR) : (convData?.type === 'utu' ? otherMember?.avatar_url : undefined));

  const displayName = convData?.type === 'self' ? "Cloud của tôi" : (item.name || convData?.name || otherMember?.name || "Cuộc trò chuyện");

  const fetchFullConversation = async (convId: string): Promise<Conversation | null> => {
    // 1. Check in O(1) cache
    const cached = chatCache.getConversation(convId);
    if (cached && cached.member_ids && cached.member_ids.length > 0) return cached;

    // 2. Fetch from API
    try {
      const res = await conversationApi.getConversationById(convId);
      const conv = (res as any).data || res;
      if (conv) chatCache.setConversation(conv);
      return conv;
    } catch (error) {
      console.error("Failed to fetch conversation details:", error);
      return null;
    }
  };

  const handleClick = async () => {
    if (item.search_type === 'user') {
      try {
        if (user?.id && item.id === user.id) {
          const selfConvId = chatCache.getSelfConversationId(user.id);
          if (selfConvId) {
            const selfConv = chatCache.getConversation(selfConvId);
            if (selfConv) setActiveConversation(selfConv);
            router.push(`${basePath}?conversation_id=${selfConvId}`);
          } else {
            router.push(`${basePath}?receiver_id=${item.id}`);
          }
          setSearchMode(false);
          return;
        }

        const res = await conversationApi.createDirectConversation([item.id]);
        const conversation = res;
        chatCache.setConversation(conversation);
        setActiveConversation({
          id: conversation.id,
          name: conversation.name,
          avatar_url: conversation.avatar_url,
          type: conversation.type || 'utu'
        } as any);
        router.push(`${basePath}?conversation_id=${conversation.id}`);
        setSearchMode(false);
      } catch (error) {
        console.error("Failed to create or fetch direct conversation:", error);
      }
    } else if (item.search_type === 'conversation') {
      const convId = item.id;
      setSearchMode(false);
      const fullConv = (convData && convData.member_ids && convData.member_ids.length > 0)
        ? convData
        : await fetchFullConversation(convId);
      if (fullConv) {
        setActiveConversation(fullConv);
      }
      router.push(`${basePath}?conversation_id=${convId}`);
    } else if (item.search_type === 'message' && item.conversation) {
      const convId = item.conversation.id;
      setTargetMessageId(item.id);
      setSearchMode(false);
      const fullConv = await fetchFullConversation(convId);
      if (fullConv) {
        setActiveConversation(fullConv);
      }
      router.push(`${basePath}?conversation_id=${convId}`);
    }
  };

  return {
    displayAvatar,
    displayName,
    handleClick,
  };
};
