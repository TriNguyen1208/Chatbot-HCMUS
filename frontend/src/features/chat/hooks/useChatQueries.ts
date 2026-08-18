import { useInfiniteQuery } from "@tanstack/react-query";
import { conversationApi } from "../api/conversation.api";
import { messageApi } from "../api/message.api";
import { Conversation, Message } from "../types";

export const useConversationsQuery = (type?: 'utu' | 'group') => {
  return useInfiniteQuery({
    queryKey: type ? ['conversations', type] : ['conversations'],
    
    queryFn: async ({ pageParam }) => {
      const res = await conversationApi.getConversations(20, pageParam as string | undefined, type);
      return (Array.isArray(res.data) ? res.data : res) as Conversation[];
    },
    
    initialPageParam: undefined as string | undefined,
    
    getNextPageParam: (lastPage: Conversation[]) => {
      if (lastPage && lastPage.length === 20) {
        const lastItem = lastPage[lastPage.length - 1];
        return lastItem.last_message?._id || (lastItem.last_message as any)?.id || lastItem._id || (lastItem as any).id;
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
        return lastItem._id || (lastItem as any).id;
      }
      return undefined;
    },
    
    enabled: !!conversationId,
  });
};
