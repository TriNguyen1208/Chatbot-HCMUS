import createHttpError from "http-errors";
import type { ConversationFacade } from "../../conversation/conversation.facade.js";
import type { MessageRepository } from "../repositories/message.repository.js";
import type { Message } from "../entities/message.entity.js";
import { socketManager } from "#@/infrastructure/websocket/socket-manager.js";
import { queueService } from "#@/modules/queue/queue.service.js";
import { checkSystemLoad } from "#@/shared/utils/system-monitor.js";
import { redisClient } from "#@/infrastructure/redis/redis.js";

// This class contains all message processing logic (Business Logic)
export class MessageService {
    constructor(
        private readonly conversationFacade: ConversationFacade,
        private readonly messageRepo: MessageRepository
    ) { }

    /**
     * Handles an incoming message payload.
     * Automatically creates a 1-1 conversation if it doesn't exist.
     * Checks permissions, system load, saves to DB, and emits socket events.
     * @param sender_id The ID of the user sending the message.
     * @param payload The message details (content, type, media attachments).
     * @returns An object indicating status ('success' or 'queued') and the message data.
     */
    async handleIncomingMessage(
        sender_id: string,
        payload: {
            conversation_id?: string;
            receiver_id?: string;
            content?: string;
            type: 'text' | 'file' | 'link' | 'image' | 'video' | 'ai' | 'system';
            tag_ids?: string[];
            image?: { url: string; file_key?: string };
            video?: { url?: string; file_key: string; thumbnail_url?: string };
            status?: 'sent' | 'received' | 'recalled' | 'removed';
        }
    ) {
        let conversation_id = payload.conversation_id;
        if (!conversation_id && payload.receiver_id) {
            const conv = await this.conversationFacade.createConversation(sender_id, {
                type: 'utu',
                member_ids: [sender_id, payload.receiver_id]
            });
            conversation_id = conv._id!.toString();
        }

        if (!conversation_id) {
            throw createHttpError.BadRequest("conversation_id or receiver_id is required");
        }

        // Step 1: Check permissions - Does the user really belong to this group?
        const isMember = await this.conversationFacade.isUserInConversation(conversation_id, sender_id);
        if (!isMember) {
            throw createHttpError.Forbidden("You are not a member of this conversation");
        }

        // Step 2: Create the base Message object
        const messageData: Message = {
            sender_id,
            conversation_id: conversation_id,
            content: payload.content,
            type: payload.type ?? 'text',
            status: payload.status ?? 'sent',
            image: payload.image,
            video: payload.video,
            tag_ids: payload.tag_ids,
            created_at: new Date()
        };

        // Step 3: Check system load
        const isOverloaded = await checkSystemLoad();
        if (isOverloaded) {
            console.warn(`[MessageService] The system is busy. Pushing messages to the queue for conversation ${conversation_id}`);
            await queueService.addJob('create_message', messageData);
            return {
                status: 'queued',
                message: "The message is being processed in the background due to the system being busy."
            };
        }

        // Step 4: Save Database directly
        const savedMessage = await this.messageRepo.create(messageData);

        // Update last message id in conversation
        await this.conversationFacade.updateLastMessage(conversation_id, savedMessage._id!);

        // Invalidate caches
        await redisClient.delByPattern(`conversation:${conversation_id}:messages:*`);
        await redisClient.del(`conversation:${conversation_id}`);
        await redisClient.delByPattern('user:*:conversations:*');

        // Step 5: Fire SocketIO to notify everyone in the chat room (conversation_id)
        socketManager.emitToGroup(conversation_id, "new_message", savedMessage);

        return {
            status: 'success',
            data: savedMessage
        };
    }

    /**
     * Retrieves a paginated list of messages for a conversation.
     * Validates that the requesting user is a member.
     * @param conversationId The ID of the conversation.
     * @param userId The ID of the requesting user.
     * @param limit The maximum number of messages.
     * @param cursorId The ID of the last fetched message for pagination.
     * @returns An array of messages.
     */
    async getMessages(conversationId: string, userId: string, limit?: number, cursorId?: string): Promise<any[]> {
        const isMember = await this.conversationFacade.isUserInConversation(conversationId, userId);
        if (!isMember) {
            throw createHttpError.Forbidden("You are not a member of this conversation");
        }

        const cacheKey = `conversation:${conversationId}:messages:${limit || 20}:${cursorId || 'start'}`;
        const cached = await redisClient.getJSON<any[]>(cacheKey);
        if (cached) return cached;

        const messages = await this.messageRepo.getMessages(conversationId, limit, cursorId);

        const result = messages.map(m => ({
            ...m,
            sender: m.sender_id === 'system' ? { id: 'system', name: 'System' } : (m as any).sender
        }));

        await redisClient.setJSON(cacheKey, result, 3600);
        return result;
    }

    /**
     * Edits an existing text message.
     * Ensures only the sender can edit and only text messages are modified.
     * @param messageId The ID of the message to edit.
     * @param userId The ID of the user attempting to edit.
     * @param newContent The updated text content.
     */
    async editMessage(messageId: string, userId: string, newContent: string) {
        const message = await this.messageRepo.findByID(messageId);
        if (!message) throw createHttpError.NotFound("Message not found");
        if (message.sender_id !== userId) throw createHttpError.Forbidden("You can only edit your own messages");
        if (message.type !== 'text') throw createHttpError.BadRequest("Only text messages can be edited");

        const updatedAt = new Date();
        await this.messageRepo.updateContent(messageId, newContent, updatedAt);

        // Invalidate caches
        const convIdStr = message.conversation?._id?.toString() as string;
        await redisClient.delByPattern(`conversation:${convIdStr}:messages:*`);
        await redisClient.del(`conversation:${convIdStr}`);
        await redisClient.delByPattern('user:*:conversations:*');

        socketManager.emitToGroup(convIdStr, "message_edited", { messageId, content: newContent, updated_at: updatedAt, conversation_id: convIdStr });
    }

    /**
     * Recalls (un-sends) a message, hiding its content.
     * @param messageId The ID of the message to recall.
     * @param userId The ID of the user attempting to recall.
     */
    async recallMessage(messageId: string, userId: string) {
        const message = await this.messageRepo.findByID(messageId) as any;
        if (!message) throw createHttpError.NotFound("Message not found");
        if (message.sender.id !== userId) throw createHttpError.Forbidden("You can only recall your own messages");

        await this.messageRepo.updateStatus(messageId, 'recalled');

        // Invalidate caches
        const convIdStr = message.conversation?._id?.toString() as string;
        await redisClient.delByPattern(`conversation:${convIdStr}:messages:*`);
        await redisClient.del(`conversation:${convIdStr}`);
        await redisClient.delByPattern('user:*:conversations:*');

        socketManager.emitToGroup(convIdStr, "message_recalled", { messageId, conversation_id: convIdStr });
    }

    /**
     * Creates and emits a system-generated message (e.g., "User joined the group").
     * @param conversationId The target conversation ID.
     * @param content The system message text.
     */
    async createSystemMessage(conversationId: string, content: string) {
        const messageData: Message = {
            sender_id: 'system',
            conversation_id: conversationId,
            content,
            type: 'system',
            status: 'sent',
            created_at: new Date()
        };
        const savedMessage = await this.messageRepo.create(messageData);
        await this.conversationFacade.updateLastMessage(conversationId, savedMessage._id!);

        // Invalidate caches
        await redisClient.delByPattern(`conversation:${conversationId}:messages:*`);
        await redisClient.del(`conversation:${conversationId}`);
        await redisClient.delByPattern('user:*:conversations:*');

        socketManager.emitToGroup(conversationId, "new_message", savedMessage);
    }

    /**
     * Finalizes a video message once background processing (e.g., FFmpeg) completes.
     * Updates the message status and emits a socket event.
     * @param fileKey The unique key of the uploaded video.
     * @param streamUrl The URL for the processed video stream (HLS).
     * @param thumbnailUrl The URL for the generated thumbnail.
     */
    async handleVideoReady(fileKey: string, streamUrl: string, thumbnailUrl: string) {
        const updatedMessage = await this.messageRepo.updateByFileKey(fileKey, {
            status: 'sent',
            video: {
                file_key: fileKey,
                url: streamUrl,
                thumbnail_url: thumbnailUrl
            }
        });

        if (updatedMessage && updatedMessage.conversation_id) {
            const convIdStr = updatedMessage.conversation_id.toString();

            // Invalidate caches
            await redisClient.delByPattern(`conversation:${convIdStr}:messages:*`);
            await redisClient.del(`conversation:${convIdStr}`);
            await redisClient.delByPattern('user:*:conversations:*');

            socketManager.emitToGroup(convIdStr, "new_message", updatedMessage);
        }
    }
}
