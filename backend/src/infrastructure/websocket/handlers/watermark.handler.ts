import type { Socket } from "socket.io";
import type { SocketManager } from "../socket.manager.js";
import { SocketEvents } from "../socket.events.js";
import type { MarkWatermarkPayload, WatermarkType } from "../socket.types.js";
import { redisClient } from "#@/infrastructure/redis/redis.client.js";
import { conversationFacade } from "#@/modules/conversation/conversation.facade.js";
import { queueService } from "#@/background/queue.service.js";

const handleWatermarkUpdate = async (
    socketManager: SocketManager,
    userId: string,
    conversationId: string,
    messageId: string,
    type: WatermarkType
): Promise<void> => {
    try {
        // 1. Write-Behind Caching: Update Redis immediately (<1ms)
        const key = `watermarks:${conversationId}`;
        const field = userId;

        const currentStr = await redisClient.getClient().hget(key, field);
        const current = currentStr ? JSON.parse(currentStr) : {};

        if (type === 'delivered') {
            current.last_delivered_msg_id = messageId;
        } else {
            current.last_read_msg_id = messageId;
        }

        await redisClient.getClient().hset(key, field, JSON.stringify(current));

        // 2. Dispatch background job to BullMQ for asynchronous database persistence
        await queueService.addJob('sync_watermark', {
            conversationId,
            userId,
            messageId,
            type
        });

        // 3. Broadcast updated watermark event to conversation members
        const members = await conversationFacade.getConversationMembers(conversationId, userId);
        socketManager.emitToUsers(members, SocketEvents.WATERMARK_UPDATED, {
            conversationId,
            userId,
            messageId,
            type
        });
    } catch (error) {
        console.error(`[Socket.IO] Error handling watermark update:`, error);
    }
};

export const registerWatermarkHandler = (socket: Socket, socketManager: SocketManager): void => {
    const userId = socket.data.userId as string;

    socket.on(SocketEvents.MARK_DELIVERED, async (data: MarkWatermarkPayload) => {
        if (!data?.conversationId || !data?.messageId) return;
        await handleWatermarkUpdate(socketManager, userId, data.conversationId, data.messageId, 'delivered');
    });

    socket.on(SocketEvents.MARK_READ, async (data: MarkWatermarkPayload) => {
        if (!data?.conversationId || !data?.messageId) return;
        await handleWatermarkUpdate(socketManager, userId, data.conversationId, data.messageId, 'read');
    });
};
