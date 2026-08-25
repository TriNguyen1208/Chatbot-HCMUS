import { create } from 'zustand';
import { Conversation } from '../types';

export interface TypingUser {
    userId: string;
    name: string;
}

export interface ChatState {
    activeConversation: Conversation | null;
    setActiveConversation: (conversation: Conversation | null) => void;
    typingUsers: Record<string, TypingUser[]>;
    addTypingUser: (conversationId: string, userId: string, name: string) => void;
    removeTypingUser: (conversationId: string, userId: string) => void;
}

export const useChatStore = create<ChatState>()((set) => ({
    activeConversation: null,
    setActiveConversation: (conversation) => set({ activeConversation: conversation }),
    typingUsers: {},
    addTypingUser: (conversationId, userId, name) => set((state) => {
        const currentUsers = state.typingUsers[conversationId] || [];
        if (currentUsers.some(u => u.userId === userId)) return state;
        return {
            typingUsers: {
                ...state.typingUsers,
                [conversationId]: [...currentUsers, { userId, name }]
            }
        };
    }),
    removeTypingUser: (conversationId, userId) => set((state) => {
        const currentUsers = state.typingUsers[conversationId] || [];
        return {
            typingUsers: {
                ...state.typingUsers,
                [conversationId]: currentUsers.filter(u => u.userId !== userId)
            }
        };
    })
}));
