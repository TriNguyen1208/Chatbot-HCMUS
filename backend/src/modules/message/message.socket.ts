import type { Socket } from "socket.io";
import type { SocketManager } from "#@/infrastructure/websocket/socket.manager.js";
import { CONVERSATION_ROOM } from "#@/infrastructure/websocket/socket.manager.js";
import { SocketEvents } from "#@/infrastructure/websocket/socket.events.js";
import type {
    TypingPayload,
    StopTypingPayload,
    SocketAckResponse
} from "#@/infrastructure/websocket/socket.types.js";
import { validateSocketPayload } from "#@/infrastructure/websocket/socket.util.js";
import {
    SendMessageSocketSchema,
    EditMessageSocketSchema,
    RecallMessageSocketSchema,
    ToggleReactionSocketSchema,
    MarkWatermarkSocketSchema
} from "./message.dto.js";
import type { Message } from "./message.entity.js";
import { messageContainer } from "./message.container.js";

/**
 * Quản lý tất cả các sự kiện Socket liên quan đến Message:
 * - Soạn thảo (Typing / Stop typing)
 * - Đã nhận / Đã đọc (Watermark: delivered, read) trực tiếp qua Service, cập nhật DB & ACK
 * - Nhắn tin thời gian thực Full-Duplex (Gửi, sửa, thu hồi, reaction) với Zod validation & ACK callback
 */
export const registerMessageSocket = (socket: Socket, socketManager: SocketManager): void => {
    const userId = socket.data.userId as string;
    if (!userId) return;

    // --- 1. Typing Handlers (O(1) room broadcasting, exclude sender) ---
    socket.on(SocketEvents.TYPING, (data: TypingPayload) => {
        if (!data?.conversationId) return;

        socket.to(CONVERSATION_ROOM(data.conversationId)).emit(SocketEvents.TYPING, {
            conversationId: data.conversationId,
            userId,
            name: data.name || "Ai đó"
        });
    });

    socket.on(SocketEvents.STOP_TYPING, (data: StopTypingPayload) => {
        if (!data?.conversationId) return;

        socket.to(CONVERSATION_ROOM(data.conversationId)).emit(SocketEvents.STOP_TYPING, {
            conversationId: data.conversationId,
            userId
        });
    });

    // --- 2. Watermark Handlers (Cập nhật trực tiếp DB, sync cache & broadcast room, kèm ACK) ---
    socket.on(SocketEvents.MARK_DELIVERED, async (rawData: unknown, ack?: (res: SocketAckResponse) => void) => {
        try {
            const data = validateSocketPayload(MarkWatermarkSocketSchema, rawData, ack);
            if (!data) return;

            const result = await messageContainer.messageService.updateWatermark(userId, {
                conversationId: data.conversationId,
                messageId: data.messageId,
                type: 'delivered'
            });

            ack?.({
                success: true,
                data: result
            });
        } catch (error: any) {
            console.error(`[MessageSocket] Error marking delivered for user ${userId}:`, error);
            ack?.({
                success: false,
                code: error.status || error.statusCode || 500,
                message: error.message || "Cập nhật trạng thái đã nhận thất bại"
            });
        }
    });

    socket.on(SocketEvents.MARK_READ, async (rawData: unknown, ack?: (res: SocketAckResponse) => void) => {
        try {
            const data = validateSocketPayload(MarkWatermarkSocketSchema, rawData, ack);
            if (!data) return;

            const result = await messageContainer.messageService.updateWatermark(userId, {
                conversationId: data.conversationId,
                messageId: data.messageId,
                type: 'read'
            });

            ack?.({
                success: true,
                data: result
            });
        } catch (error: any) {
            console.error(`[MessageSocket] Error marking read for user ${userId}:`, error);
            ack?.({
                success: false,
                code: error.status || error.statusCode || 500,
                message: error.message || "Cập nhật trạng thái đã đọc thất bại"
            });
        }
    });

    // --- 3. Full-Duplex Realtime Message Mutations with Zod Validation & ACK ---

    // Gửi tin nhắn
    socket.on(SocketEvents.NEW_MESSAGE, async (rawData: unknown, ack?: (res: SocketAckResponse<Message>) => void) => {
        try {
            const data = validateSocketPayload(SendMessageSocketSchema, rawData, ack);
            if (!data) return;

            const result = await messageContainer.messageService.handleIncomingMessage(userId, data);
            ack?.({
                success: true,
                data: result.data,
                message: result.message
            });
        } catch (error: any) {
            console.error(`[MessageSocket] Error sending message for user ${userId}:`, error);
            ack?.({
                success: false,
                code: error.status || error.statusCode || 500,
                message: error.message || "Gửi tin nhắn thất bại"
            });
        }
    });

    // Sửa tin nhắn
    socket.on(SocketEvents.EDIT_MESSAGE, async (rawData: unknown, ack?: (res: SocketAckResponse<Message>) => void) => {
        try {
            const data = validateSocketPayload(EditMessageSocketSchema, rawData, ack);
            if (!data) return;

            const updated = await messageContainer.messageService.editMessage(data.message_id, userId, data.content);
            ack?.({
                success: true,
                data: updated
            });
        } catch (error: any) {
            console.error(`[MessageSocket] Error editing message for user ${userId}:`, error);
            ack?.({
                success: false,
                code: error.status || error.statusCode || 500,
                message: error.message || "Sửa tin nhắn thất bại"
            });
        }
    });

    // Thu hồi tin nhắn
    socket.on(SocketEvents.RECALL_MESSAGE, async (rawData: unknown, ack?: (res: SocketAckResponse<Message>) => void) => {
        try {
            const data = validateSocketPayload(RecallMessageSocketSchema, rawData, ack);
            if (!data) return;

            const recalled = await messageContainer.messageService.recallMessage(data.message_id, userId);
            ack?.({
                success: true,
                data: recalled
            });
        } catch (error: any) {
            console.error(`[MessageSocket] Error recalling message for user ${userId}:`, error);
            ack?.({
                success: false,
                code: error.status || error.statusCode || 500,
                message: error.message || "Thu hồi tin nhắn thất bại"
            });
        }
    });

    // Thả cảm xúc Reaction
    socket.on(SocketEvents.REACTION_MESSAGE, async (rawData: unknown, ack?: (res: SocketAckResponse<Message>) => void) => {
        try {
            const data = validateSocketPayload(ToggleReactionSocketSchema, rawData, ack);
            if (!data) return;

            const updated = await messageContainer.messageService.toggleReaction(data.message_id, userId, data.emoji);
            ack?.({
                success: true,
                data: updated
            });
        } catch (error: any) {
            console.error(`[MessageSocket] Error toggling reaction for user ${userId}:`, error);
            ack?.({
                success: false,
                code: error.status || error.statusCode || 500,
                message: error.message || "Thả reaction thất bại"
            });
        }
    });
};

