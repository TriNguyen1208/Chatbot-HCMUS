import { http } from "@/lib/api";
import type { Conversation } from "@/types";

export const conversationApi = {
    getConversations: async (limit: number = 20, cursorId?: string, type?: 'utu' | 'group'): Promise<Conversation[]> => {
        const params: Record<string, any> = { limit };
        if (cursorId) params.cursor_id = cursorId;
        if (type) params.type = type;
        return http.get<Conversation[]>('/conversation', { params });
    },

    createGroup: async (name: string, member_ids: string[]): Promise<Conversation> => {
        return http.post<Conversation>('/conversation', {
            type: 'group',
            name,
            member_ids,
        });
    },

    createDirectConversation: async (member_ids: string[]): Promise<Conversation> => {
        return http.post<Conversation>('/conversation', {
            type: 'utu',
            member_ids,
        });
    },

    getConversationById: async (id: string): Promise<Conversation> => {
        return http.get<Conversation>(`/conversation/${id}`);
    },

    updateConversation: async (
        id: string,
        data: { name?: string; avatar_url?: string; primary_icon?: string }
    ): Promise<Conversation> => {
        return http.put<Conversation>(`/conversation/${id}`, data);
    },

    removeMembers: async (id: string, member_ids: string[]): Promise<Conversation> => {
        return http.delete<Conversation>(`/conversation/${id}/members`, {
            data: { member_ids },
        });
    },

    assignAdmins: async (id: string, admin_ids: string[]): Promise<Conversation> => {
        return http.post<Conversation>(`/conversation/${id}/admins`, { admin_ids });
    },

    leaveGroup: async (id: string): Promise<{ success: boolean }> => {
        return http.post<{ success: boolean }>(`/conversation/${id}/leave`);
    },

    addMembers: async (id: string, member_ids: string[]): Promise<Conversation> => {
        return http.post<Conversation>(`/conversation/${id}/members`, { member_ids });
    },

    blockConversation: async (id: string): Promise<Conversation> => {
        return http.post<Conversation>(`/conversation/${id}/block`);
    },

    unblockConversation: async (id: string): Promise<Conversation> => {
        return http.post<Conversation>(`/conversation/${id}/unblock`);
    },

    disbandGroup: async (id: string): Promise<{ success: boolean }> => {
        return http.delete<{ success: boolean }>(`/conversation/${id}`);
    },
};
