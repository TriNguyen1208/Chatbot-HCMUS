import { api } from "@/lib/api";

import { Conversation } from '../types';

export const conversationApi = {
    getConversations: async (limit: number = 20, cursorId?: string, type?: 'utu' | 'group') => {
        const params: Record<string, any> = { limit };
        
        if (cursorId) params.cursor_id = cursorId;
        if (type) params.type = type;
        
        const response = await api.get('/conversation', { params });
        
        return response.data;
    },
    createGroup: async (name: string, member_ids: string[]) => {
        const response = await api.post('/conversation', {
            type: 'group',
            name,          
            member_ids    
        });
        
        return response.data;
    },
    getConversationById: async (id: string) => {
        const response = await api.get(`/conversation/${id}`);
        
        return response.data;
    },
    removeMembers: async (id: string, member_ids: string[]) => {
        const response = await api.delete(`/conversation/${id}/members`, { data: { member_ids } } as any);
        return response.data;
    },
    assignAdmins: async (id: string, admin_ids: string[]) => {
        const response = await api.post(`/conversation/${id}/admins`, { admin_ids });
        return response.data;
    },
    leaveGroup: async (id: string) => {
        const response = await api.post(`/conversation/${id}/leave`);        
        return response.data;
    }
};
