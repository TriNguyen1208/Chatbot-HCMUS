import createHttpError from "http-errors";
import type { ConversationFacade } from "../../conversation/conversation.facade.js";
import type { MessageRepository } from "../repositories/message.repository.js";
import type { Message, MessageDB } from "../entities/message.entity.js";
import { socketManager } from "#@/infrastructure/websocket/socket-manager.js";
import { queueService } from "#@/modules/queue/queue.service.js";
import { checkSystemLoad } from "#@/shared/utils/system-monitor.js";
import { redisClient } from "#@/infrastructure/redis/redis.js";
import type { SendMessageDto } from "../dto/message.dto.js";

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
        payload: SendMessageDto
    ) {
        let conversation_id = payload.conversation_id;
        if (!conversation_id && payload.receiver_id) {
            const conv = await this.conversationFacade.createConversation(sender_id, {
                type: 'utu',
                member_ids: [sender_id, payload.receiver_id.toString()]
            });
            conversation_id = conv.id!.toString();
        }

        if (!conversation_id) {
            throw createHttpError.BadRequest("conversation_id or receiver_id is required");
        }

        // Step 1: Check permissions - Does the user really belong to this group?
        const isMember = await this.conversationFacade.isUserInConversation(conversation_id.toString(), sender_id);
        if (!isMember) {
            throw createHttpError.Forbidden("You are not a member of this conversation");
        }

        // Step 2: Create the base Message object
        const messageData: MessageDB = {
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
        await this.conversationFacade.updateLastMessage(conversation_id.toString(), savedMessage.id!);

        // Invalidate caches
        await redisClient.del(`conversation:${conversation_id}`);

        // Lazy join members before emitting
        const members = await this.conversationFacade.getConversationMembers(conversation_id.toString(), sender_id);
        members.forEach(memberId => {
            socketManager.joinGroup(memberId, conversation_id.toString());
        });

        // Step 5: Fire SocketIO to notify everyone in the chat room (conversation_id)
        socketManager.emitToGroup(conversation_id.toString(), "new_message", savedMessage);
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
        const messages = await this.messageRepo.getMessages(conversationId, limit, cursorId);

        const result = messages.map(m => ({
            ...m,
            sender: m.type === 'system' ? { id: 'system', name: 'System' } : m.sender_id?.toString()
        }));

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
        if (message.sender_id?.toString() !== userId) throw createHttpError.Forbidden("You can only edit your own messages");
        if (message.type !== 'text') throw createHttpError.BadRequest("Only text messages can be edited");

        const updatedAt = new Date();
        await this.messageRepo.updateContent(messageId, newContent, updatedAt);

        // Invalidate caches
        const convIdStr = message.conversation_id.toString();
        await redisClient.del(`conversation:${convIdStr}`);

        socketManager.emitToGroup(convIdStr, "message_edited", { messageId, content: newContent, updated_at: updatedAt, conversation_id: convIdStr });
    }

    /**
     * Recalls (un-sends) a message, hiding its content.
     * @param messageId The ID of the message to recall.
     * @param userId The ID of the user attempting to recall.
     */
    async recallMessage(messageId: string, userId: string) {
        const message = await this.messageRepo.findByID(messageId);
        if (!message) throw createHttpError.NotFound("Message not found");
        if (message.sender_id?.toString() !== userId) throw createHttpError.Forbidden("You can only recall your own messages");

        await this.messageRepo.updateStatus(messageId, 'recalled');

        // Invalidate caches
        const convIdStr = message.conversation_id?.toString() as string;
        await redisClient.del(`conversation:${convIdStr}`);

        socketManager.emitToGroup(convIdStr, "message_recalled", { messageId, conversation_id: convIdStr });
    }

    /**
     * Creates and emits a system-generated message (e.g., "User joined the group").
     * @param conversationId The target conversation ID.
     * @param content The system message text.
     */
    async createSystemMessage(conversationId: string, content: string) {
        const messageData: MessageDB = {
            conversation_id: conversationId,
            content,
            type: 'system',
            status: 'sent',
            created_at: new Date()
        };
        const savedMessage = await this.messageRepo.create(messageData);
        await this.conversationFacade.updateLastMessage(conversationId, savedMessage.id!);

        // Invalidate caches
        await redisClient.del(`conversation:${conversationId}`);

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
            const convIdStr = updatedMessage.conversation_id!.toString();

            // Invalidate caches
            await redisClient.del(`conversation:${convIdStr}`);

            socketManager.emitToGroup(convIdStr, "new_message", updatedMessage);
        }
    }

    /**
     * Toggles a reaction for a message.
     * Checks if the user is part of the conversation before allowing the reaction.
     * Emits socket event to notify other users.
     */
    async toggleReaction(messageId: string, userId: string, emoji: string): Promise<Message> {
        // First get the message to verify conversation membership
        const message = await this.messageRepo.findByID(messageId);
        if (!message) {
            throw createHttpError(404, 'Message not found');
        }

        // Toggle the reaction
        const updatedMessage = await this.messageRepo.toggleReaction(message, userId, emoji);

        // Invalidate caches

        const convIdStr = message.conversation_id.toString()
        // Emit socket event
        socketManager.emitToGroup(convIdStr, "message_reaction_updated", {
            message_id: updatedMessage.id,
            reactions: updatedMessage.reactions,
            conversation_id: convIdStr
        });

        return updatedMessage;
    }
}
