import { http } from "@/lib/api";
import { socketService } from "@/shared/services/socket.service";
import type { Conversation } from "@/types";

export const conversationApi = {
    getConversations: async (limit: number = 20, cursorId?: string, type?: 'utu' | 'group'): Promise<Conversation[]> => {
        const params: Record<string, any> = { limit };
        if (cursorId) params.cursor_id = cursorId;
        if (type) params.type = type;
        return http.get<Conversation[]>('/conversation', { params });
    },

    createGroup: async (name: string, member_ids: string[]): Promise<Conversation> => {
        return socketService.emitWithAck<Conversation>('new_conversation', {
            type: 'group',
            name,
            member_ids,
        });
    },

    createDirectConversation: async (member_ids: string[]): Promise<Conversation> => {
        return socketService.emitWithAck<Conversation>('new_conversation', {
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
        return socketService.emitWithAck<Conversation>('conversation_updated', {
            conversation_id: id,
            ...data,
        });
    },

    removeMembers: async (id: string, member_ids: string[]): Promise<Conversation> => {
        return socketService.emitWithAck<Conversation>('members_kicked', {
            conversation_id: id,
            member_ids,
        });
    },

    assignAdmins: async (id: string, admin_ids: string[]): Promise<Conversation> => {
        return socketService.emitWithAck<Conversation>('admins_updated', {
            conversation_id: id,
            admin_ids,
        });
    },

    leaveGroup: async (id: string): Promise<{ success: boolean }> => {
        return socketService.emitWithAck<{ success: boolean }>('member_left', {
            conversation_id: id,
        });
    },

    addMembers: async (id: string, member_ids: string[]): Promise<Conversation> => {
        return socketService.emitWithAck<Conversation>('members_added', {
            conversation_id: id,
            member_ids,
        });
    },

    blockConversation: async (id: string): Promise<Conversation> => {
        return socketService.emitWithAck<Conversation>('conversation_blocked', {
            conversation_id: id,
        });
    },

    unblockConversation: async (id: string): Promise<Conversation> => {
        return socketService.emitWithAck<Conversation>('conversation_unblocked', {
            conversation_id: id,
        });
    },

    disbandGroup: async (id: string): Promise<{ success: boolean }> => {
        return socketService.emitWithAck<{ success: boolean }>('group_disbanded', {
            conversation_id: id,
        });
    },
};

