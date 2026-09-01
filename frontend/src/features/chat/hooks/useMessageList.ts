import { useEffect } from "react";
import { useInView } from "react-intersection-observer";
import { useChatStore } from "../stores/chatStore";
import { useMessagesQuery } from "./useChatQueries";

export const useMessageList = () => {
    const { activeConversation } = useChatStore();
    const activeConversationId = activeConversation?.id || activeConversation?.id;
    
    const { data, fetchNextPage, hasNextPage, isFetchingNextPage, isLoading } = useMessagesQuery(activeConversationId);
    
    const { ref, inView } = useInView();

    const messages = data?.pages.flatMap(page => page) || [];

    useEffect(() => {
        if (inView && hasNextPage && !isFetchingNextPage && messages.length > 0) {
            fetchNextPage();
        }
    }, [inView, hasNextPage, isFetchingNextPage, fetchNextPage, messages.length]);

    return {
        messages,
        isLoadingMessages: isLoading,
        hasMoreMessages: hasNextPage,
        ref
    };
};
