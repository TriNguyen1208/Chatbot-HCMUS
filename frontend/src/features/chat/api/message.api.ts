import { http } from "@/lib/api";
import { socketService } from "@/shared/services/socket.service";
import type { Message } from "@/types";

export interface SendMessagePayload {
    conversation_id?: string;
    receiver_id?: string;
    content?: string;
    type?: string;
    image?: { url: string; file_key?: string };
    video?: { url?: string; file_key: string; thumbnail_url?: string };
    client_msg_id?: string;
}

export const messageApi = {
    //Get list message in conversation
    getMessages: async (
        conversationId: string,
        limit: number = 20,
        cursorId?: string,
        search?: string,
        type?: string,
    ): Promise<Message[]> => {
        const params: Record<string, unknown> = { limit };
        if (cursorId) params.cursor_id = cursorId;
        if (search) params.search = search;
        if (type) params.type = type;
        return http.get<Message[]>(`/message/${conversationId}`, { params });
    },

    //Get before and after message in conversation
    getContextMessages: async (
        conversationId: string,
        messageId: string,
        limit: number = 10,
    ): Promise<Message[]> => {
        const params: Record<string, unknown> = { limit };
        return http.get<Message[]>(
            `/message/${conversationId}/context/${messageId}`,
            { params },
        );
    },

    //Send message
    sendMessage: async (payload: SendMessagePayload): Promise<Message> => {
        const payloadWithId: SendMessagePayload = {
            ...payload,
            client_msg_id: payload.client_msg_id || crypto.randomUUID(),
        };
        return socketService.emitWithAck<Message>("new_message", payloadWithId);
    },

    //Edit message
    editMessage: async (id: string, content: string): Promise<Message> => {
        return socketService.emitWithAck<Message>("message_edited", {
            message_id: id,
            content,
        });
    },

    //Recall message
    recallMessage: async (id: string): Promise<Message> => {
        return socketService.emitWithAck<Message>("message_recalled", {
            message_id: id,
        });
    },

    //Toggle reaction
    toggleReaction: async (id: string, emoji: string): Promise<Message> => {
        return socketService.emitWithAck<Message>("message_reaction_updated", {
            message_id: id,
            emoji,
        });
    },
};
