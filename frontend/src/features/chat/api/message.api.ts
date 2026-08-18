// Import instance axios đã được config sẵn
import { api } from "@/lib/api";

import { Message } from '../types';

export const messageApi = {
    getMessages: async (conversationId: string, limit: number = 20, cursorId?: string) => {
        const params: Record<string, any> = { limit };
        if (cursorId) params.cursor_id = cursorId;
        const response = await api.get(`/message/${conversationId}`, { params });
        return response.data;
    },

    sendMessage: async (
        payload: { 
            conversation_id?: string; 
            receiver_id?: string; 
            content?: string; 
            type?: string; 
            image?: { url: string; file_key?: string }; 
            video?: { url?: string; file_key: string; thumbnail_url?: string }; 
        }) => {
        const response = await api.post('/message', payload);
        return response.data;
    },

    editMessage: async (id: string, content: string) => {
        const response = await api.put(`/message/${id}`, { content });
        return response.data;
    },

    recallMessage: async (id: string) => {
        const response = await api.delete(`/message/${id}/recall`);
        return response.data;
    }
};
