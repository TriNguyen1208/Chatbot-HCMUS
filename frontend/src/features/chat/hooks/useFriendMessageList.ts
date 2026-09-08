import { useEffect } from "react";
import { useInView } from "react-intersection-observer";
import { usePathname, useRouter } from "next/navigation";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { Conversation } from "@/features/chat/types";
import { useConversationsQuery } from "./useChatQueries";

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
        const convId = conv.id || (conv as any)._id;
        if (lastMsg && lastMsg.sender_id !== user.id) {
          const msgId = lastMsg.id || (lastMsg as any)._id;
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
    router.push(`?conversation_id=${conv.id}`);
    // if (conv.type === 'utu') {
    //   const receiverId = conv.members?.find((m: string) => m !== user?.id)?.id || conv.members?.[0]?.id;
    //   router.push(`?receiver_id=${receiverId}`);
    // } else {
      
    // }
  };

  return {
    conversations,
    isLoadingConversations: isLoading,
    activeConversation,
    ref,
    handleConversationClick
  };
};
