import { useEffect } from "react";
import { useInView } from "react-intersection-observer";
import { usePathname, useRouter } from "next/navigation";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useConversationsQuery } from "./useChatQueries";

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

  const { ref, inView } = useInView({ threshold: 0 });

  const conversations = data?.pages.flatMap(page => page) || [];

  useEffect(() => {
    
    if (inView && hasNextPage && !isFetchingNextPage) {
      fetchNextPage();
    }
  }, [inView, hasNextPage, isFetchingNextPage, fetchNextPage]);

  const handleConversationClick = (conv: any) => {
    setActiveConversation(conv);
    router.push(`?conversation_id=${conv._id || conv.id}`);
    // if (conv.type === 'utu') {
    //   const receiverId = conv.members?.find((m: any) => m.id !== user?.id)?.id || conv.members?.[0]?.id;
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
