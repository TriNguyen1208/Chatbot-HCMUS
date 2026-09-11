import { useChatStore } from "@/features/chat/stores/chatStore";

export const useChatArea = () => {
    const activeConversation = useChatStore(state => state.activeConversation);
    return { activeConversation };
};
