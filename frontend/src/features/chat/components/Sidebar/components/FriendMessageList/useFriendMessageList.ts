"use client";
import { useEffect } from "react";
import { useInView } from "react-intersection-observer";
import { usePathname, useRouter } from "next/navigation";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { Conversation } from "@/types";
import { useConversationsQuery } from "@/features/chat/hooks/useChatQueries";
import { useSocketContext } from "@/providers/SocketProvider";

export const useFriendMessageList = () => {
  const { activeConversation, setActiveConversation } = useChatStore();
  const pathname = usePathname();
  let typeFilter: 'group' | 'utu' | undefined = undefined;
  if (pathname.includes('/group-chat')) {
    typeFilter = 'group';
  } else if (pathname.includes('/direct-chat')) {
    typeFilter = 'utu';
  }
  const { data, fetchNextPage, hasNextPage, isFetchingNextPage, isLoading } = useConversationsQuery(typeFilter);
  const router = useRouter();
  const { user } = useAuthStore();
  const { socket } = useSocketContext();

  const { ref, inView } = useInView({ threshold: 0 });

  const conversations = data?.pages.flatMap(page => page) || [];

  // Catch-up mark_delivered logic
  useEffect(() => {
    if (conversations.length > 0 && socket && user?.id) {
      conversations.forEach(conv => {
        const lastMsg = conv.last_message;
        const convId = conv.id;
        if (lastMsg && lastMsg.sender_id !== user.id) {
          const msgId = lastMsg.id;
          if (!msgId) return;

          const myWatermark = conv.watermarks?.find(w => w.user_id === user.id);
          const isDeliveredOrRead = myWatermark?.last_delivered_msg_id === msgId || myWatermark?.last_read_msg_id === msgId;

          if (!isDeliveredOrRead && activeConversation?.id !== convId) {
            socket.emit('mark_delivered', { conversationId: convId, messageId: msgId });
          }
        }
      });
    }
  }, [conversations, socket, user?.id, activeConversation?.id]);

  useEffect(() => {
    if (inView && hasNextPage && !isFetchingNextPage) {
      fetchNextPage();
    }
  }, [inView, hasNextPage, isFetchingNextPage, fetchNextPage]);

  const handleConversationClick = (conv: Conversation) => {
    setActiveConversation(conv);
    const basePath = pathname.startsWith('/direct-chat') || pathname.startsWith('/group-chat') || pathname.startsWith('/chat')
      ? pathname
      : '/chat';
    router.push(`${basePath}?conversation_id=${conv.id}`);
  };

  return {
    conversations,
    isLoadingConversations: isLoading,
    activeConversation,
    ref,
    handleConversationClick
  };
};
