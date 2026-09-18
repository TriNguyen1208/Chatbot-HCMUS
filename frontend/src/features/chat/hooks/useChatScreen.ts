import { useActiveConversation } from "./useActiveConversation";

export const useChatScreen = (type?: "utu" | "group" | "all") => {
    const { activeConversation, isDraft, conversationId, receiverId } = useActiveConversation(type);
    return { activeConversation, isDraft, conversationId, receiverId };
};
