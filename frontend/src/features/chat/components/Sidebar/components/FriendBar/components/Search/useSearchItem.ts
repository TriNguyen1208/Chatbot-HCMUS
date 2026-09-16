"use client";

import { useEffect } from "react";
import { useRouter, usePathname } from "next/navigation";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { SearchResult } from "@/features/chat/api/search.api";
import { conversationApi } from "@/features/chat/api/conversation.api";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useSearchStore } from "@/features/chat/stores/searchStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useUserStore } from "@/features/chat/stores/userStore";
import { Conversation } from "@/types";

export const useSearchItem = (item: SearchResult) => {
  const { setActiveConversation } = useChatStore();
  const { setTargetMessageId, setSearchMode } = useSearchStore();
  const { user } = useAuthStore();
  const { users, requestUser } = useUserStore();
  const queryClient = useQueryClient();
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
      return (res as any).data || res;
    },
    enabled: isConversation && !!item.id,
    initialData: () => {
      if (!isConversation || !item.id) return undefined;
      const allCaches = queryClient.getQueriesData<{ pages: Conversation[][] }>({ queryKey: ['conversations'] });
      for (const [_, data] of allCaches) {
        if (data?.pages) {
          const found = data.pages.flat().find(c => c.id === item.id);
          if (found) return found;
        }
      }
      return undefined;
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
    || (convData?.type === 'utu' ? otherMember?.avatar_url : undefined);

  const displayName = item.name || convData?.name || otherMember?.name || "Cuộc trò chuyện";

  const fetchFullConversation = async (convId: string): Promise<Conversation | null> => {
    // 1. Check in existing query caches
    const allCaches = queryClient.getQueriesData<{ pages: Conversation[][] }>({ queryKey: ['conversations'] });
    for (const [_, data] of allCaches) {
      if (data?.pages) {
        const found = data.pages.flat().find(c => c.id === convId);
        if (found && found.member_ids && found.member_ids.length > 0) return found;
      }
    }
    // 2. Fetch from API
    try {
      const res = await conversationApi.getConversationById(convId);
      return (res as any).data || res;
    } catch (error) {
      console.error("Failed to fetch conversation details:", error);
      return null;
    }
  };

  const handleClick = async () => {
    if (item.search_type === 'user') {
      try {
        const res = await conversationApi.createDirectConversation([item.id]);
        const conversation = res;
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
