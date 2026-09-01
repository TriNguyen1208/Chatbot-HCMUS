import { useInfiniteQuery } from "@tanstack/react-query";
import { conversationApi } from "../api/conversation.api";
import { messageApi } from "../api/message.api";
import { Conversation, Message } from "../types";
import { useUserStore } from "../stores/userStore";
import { userApi } from "../api/user.api";

export const useConversationsQuery = (type?: 'utu' | 'group') => {
  return useInfiniteQuery({
    queryKey: type ? ['conversations', type] : ['conversations'],
    
    queryFn: async ({ pageParam }) => {
      const res = await conversationApi.getConversations(20, pageParam as string | undefined, type);
      const conversations = (Array.isArray(res.data) ? res.data : res) as Conversation[];
      
      const userStoreState = useUserStore.getState();
      const existingUsers = userStoreState.users;

      conversations.forEach((conv) => {
        conv.member_ids?.forEach((memberId) => {
          if (!existingUsers[memberId]) {
            userStoreState.requestUser(memberId);
          }
        });
        if (conv.last_message?.sender_id && !existingUsers[conv.last_message.sender_id]) {
          userStoreState.requestUser(conv.last_message.sender_id);
        }
      });

      return conversations;
    },
    
    initialPageParam: undefined as string | undefined,
    
    getNextPageParam: (lastPage: Conversation[]) => {
      if (lastPage && lastPage.length === 20) {
        const lastItem = lastPage[lastPage.length - 1];
        return lastItem.last_message?.id || lastItem.id;
      }
      return undefined;
    }
  });
};

export const useMessagesQuery = (conversationId?: string) => {
  return useInfiniteQuery({
    queryKey: ['messages', conversationId],
    
    queryFn: async ({ pageParam }) => {
      if (!conversationId) return [];
      
      const res = await messageApi.getMessages(conversationId, 20, pageParam as string | undefined);
      return (Array.isArray(res.data) ? res.data : res) as Message[];
    },
    
    initialPageParam: undefined as string | undefined,
    
    getNextPageParam: (lastPage: Message[]) => {
      if (lastPage && lastPage.length === 20) {
        const lastItem = lastPage[lastPage.length - 1];
        return lastItem.id;
      }
      return undefined;
    },
    
    enabled: !!conversationId,
  });
};
