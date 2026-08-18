import { create } from 'zustand';
import { Conversation } from '../types';

export interface ChatState {
    activeConversation: Conversation | null;
    setActiveConversation: (conversation: Conversation | null) => void;
}

export const useChatStore = create<ChatState>()((set) => ({
    activeConversation: null,
    setActiveConversation: (conversation) => set({ activeConversation: conversation }),
}));
