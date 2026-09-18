import createHttpError from "http-errors";
import type { ConversationFacade } from "#@/modules/conversation/conversation.facade.js";
import type { MessageRepository } from "./message.repository.js";
import type { Message, MessageDB } from "./message.entity.js";
import { socketManager } from "#@/infrastructure/websocket/socket.manager.js";
import { SocketEvents } from "#@/infrastructure/websocket/socket.events.js";
import { queueService } from "#@/background/queue.service.js";
import { checkSystemLoad } from "#@/shared/utils/system-monitor.util.js";
import type { SendMessageDto } from "./message.dto.js";
import { triggerSync, SyncOperation } from "#@/shared/utils/sync.util.js";
import { MessageCache } from "./message.cache.js";

// This class contains all message processing logic (Business Logic)
export class MessageService {
    constructor(
        private readonly conversationFacade: ConversationFacade,
        private readonly messageRepo: MessageRepository,
        private readonly messageCache: MessageCache,
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
    ): Promise<{ status: 'success'; data: Message; message?: string } | { status: 'queued'; message: string; data?: Message }> {
        let conversation_id = payload.conversation_id;
        if (!conversation_id && payload.receiver_id) {
            if (payload.receiver_id.toString() === sender_id) {
                const conv = await this.conversationFacade.findOrCreateSelfConversation(sender_id);
                conversation_id = conv.id!.toString();
            } else {
                const conv = await this.conversationFacade.createConversation(sender_id, {
                    type: 'utu',
                    member_ids: [sender_id, payload.receiver_id.toString()]
                });
                conversation_id = conv.id!.toString();
            }
        }

        if (!conversation_id) {
            throw createHttpError.BadRequest("conversation_id or receiver_id is required");
        }

        // Step 1: Check permissions - Does the user really belong to this group?
        const conv = await this.conversationFacade.getConversationById(conversation_id.toString(), sender_id);
        if (!conv) {
            throw createHttpError.Forbidden("You are not a member of this conversation");
        }

        if (conv.is_active === false) {
            throw createHttpError.Forbidden("Nhóm này đã bị giải tán, không thể gửi tin nhắn");
        }

        if (conv.block) {
            throw createHttpError.Forbidden("Cuộc trò chuyện đang bị chặn, không thể gửi tin nhắn");
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
        // // Step 3: Check system load
        // const isOverloaded = await checkSystemLoad();
        // if (isOverloaded) {
        //     console.warn(`[MessageService] The system is busy. Pushing messages to the queue for conversation ${conversation_id}`);
        //     await queueService.addJob('create_message', messageData);
        //     return {
        //         status: 'queued',
        //         message: "The message is being processed in the background due to the system being busy."
        //     };
        // }

        // Step 4: Save Database directly
        const savedMessage = await this.messageRepo.create(messageData);

        // Update last message id in conversation
        await this.conversationFacade.updateLastMessage(conversation_id.toString(), savedMessage.id!);

        // Push to dynamic message cache (50 recent messages)
        const formattedSaved = {
            ...savedMessage,
            sender: savedMessage.type === 'system' ? { id: 'system', name: 'System' } : savedMessage.sender_id?.toString()
        };
        await this.messageCache.pushRecent(conversation_id.toString(), formattedSaved);

        socketManager.emitToGroup(conversation_id.toString(), "new_message", savedMessage);

        triggerSync('messages', SyncOperation.CREATE, savedMessage);

        return {
            status: 'success',
            data: savedMessage
        };
    }

    /**
     * Gửi tin nhắn siêu tốc (Instant ACK & Non-blocking DB save):
     * 1. Validate & kiểm tra quyền trong RAM (Redis ~0.1ms).
     * 2. Tự sinh _id trên RAM (new Types.ObjectId()).
     * 3. Ghi vào Redis cache & phát Socket vào Room O(1) (~1ms).
     * 4. Trả về ngay đối tượng Message cho Socket Handler để gọi ACK (<1.5ms).
     * 5. Ghi ngầm vào MongoDB và Elasticsearch trong Event Loop mà không block client.
     */
    // async sendMessageFast(sender_id: string, payload: SendMessageDto): Promise<Message> {
    //     let conversation_id = payload.conversation_id;
    //     if (!conversation_id && payload.receiver_id) {
    //         const conv = await this.conversationFacade.createConversation(sender_id, {
    //             type: 'utu',
    //             member_ids: [sender_id, payload.receiver_id.toString()]
    //         });
    //         conversation_id = conv.id!.toString();
    //     }

    //     if (!conversation_id) {
    //         throw createHttpError.BadRequest("conversation_id or receiver_id is required");
    //     }

    //     const convIdStr = conversation_id.toString();

    //     // Kiểm tra quyền thành viên (O(1) từ Redis/Facade)
    //     const conv = await this.conversationFacade.getConversationById(convIdStr, sender_id);
    //     if (!conv) {
    //         throw createHttpError.Forbidden("You are not a member of this conversation");
    //     }
    //     if (conv.is_active === false) {
    //         throw createHttpError.Forbidden("Nhóm này đã bị giải tán, không thể gửi tin nhắn");
    //     }
    //     if (conv.block) {
    //         throw createHttpError.Forbidden("Cuộc trò chuyện đang bị chặn, không thể gửi tin nhắn");
    //     }

    //     // Sinh trước ObjectId trên RAM
    //     const pregeneratedId = new Types.ObjectId();
    //     const createdAt = new Date();

    //     const messageData: MessageDB = {
    //         _id: pregeneratedId,
    //         sender_id,
    //         conversation_id: convIdStr,
    //         content: payload.content,
    //         type: payload.type ?? 'text',
    //         status: payload.status ?? 'sent',
    //         image: payload.image,
    //         video: payload.video,
    //         tag_ids: payload.tag_ids,
    //         created_at: createdAt
    //     };

    //     const domainMessage: Message = {
    //         id: pregeneratedId.toString(),
    //         sender_id,
    //         conversation_id: convIdStr,
    //         content: payload.content,
    //         type: payload.type ?? 'text',
    //         status: payload.status ?? 'sent',
    //         image: payload.image,
    //         video: payload.video,
    //         tag_ids: payload.tag_ids,
    //         created_at: createdAt
    //     };

    //     // 1. Cập nhật cache động (50 tin nhắn mới nhất)
    //     const formattedSaved = {
    //         ...domainMessage,
    //         sender: domainMessage.type === 'system' ? { id: 'system', name: 'System' } : domainMessage.sender_id?.toString()
    //     };
    //     await this.messageCache.pushRecent(convIdStr, formattedSaved);

    //     // 2. Phát Socket O(1) vào Room cho các thành viên
    //     socketManager.emitToGroup(convIdStr, "new_message", domainMessage);

    //     // 3. Ghi DB ngầm phía sau trong Event Loop (Non-blocking)
    //     this.messageRepo.create(messageData)
    //         .then(async (saved) => {
    //             await this.conversationFacade.updateLastMessage(convIdStr, saved.id!);
    //             triggerSync('messages', SyncOperation.CREATE, saved);
    //         })
    //         .catch((err) => {
    //             console.error(`[MessageService] Background DB save error for message ${domainMessage.id}:`, err);
    //             socketManager.emitToGroup(convIdStr, "message_save_failed", {
    //                 conversationId: convIdStr,
    //                 messageId: domainMessage.id,
    //                 senderId: sender_id,
    //                 error: err.message
    //             });
    //         });

    //     return domainMessage;
    // }

    /**
     * Creates a message from background queue (deferred write under high load).
     */
    async createMessageFromQueue(messageData: any): Promise<Message> {
        const savedMessage = await this.messageRepo.create(messageData);
        await this.conversationFacade.updateLastMessage(messageData.conversation_id.toString(), savedMessage.id!);
        const formattedSaved = {
            ...savedMessage,
            sender: savedMessage.type === 'system' ? { id: 'system', name: 'System' } : savedMessage.sender_id?.toString()
        };
        await this.messageCache.pushRecent(messageData.conversation_id.toString(), formattedSaved);
        triggerSync('messages', SyncOperation.CREATE, savedMessage);
        return savedMessage;
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
    async getMessages(conversationId: string, userId: string, limit?: number, cursorId?: string, type?: string): Promise<any[]> {
        const isMember = await this.conversationFacade.isUserInConversation(conversationId, userId);
        if (!isMember) {
            throw createHttpError.Forbidden("You are not a member of this conversation");
        }

        // Ưu tiên đọc từ Redis List conv:{id}:recent (50 tin gần nhất, 0.5ms) khi đọc trang đầu tiên
        if (!cursorId && !type) {
            const cached = await this.messageCache.getRecent(conversationId, limit ?? 20);
            if (cached && cached.length > 0) {
                return cached;
            }
        }

        const messages = await this.messageRepo.getMessages(conversationId, limit, cursorId, type);

        const result = messages.map(m => ({
            ...m,
            sender: m.sender_id?.toString()
        }));

        if (!cursorId && !type && result.length > 0) {
            this.messageCache.setRecent(conversationId, result).catch(() => { });
        }

        return result;
    }

    async getContextMessages(conversationId: string, messageId: string, userId: string, limit?: number): Promise<any[]> {
        const isMember = await this.conversationFacade.isUserInConversation(conversationId, userId);
        if (!isMember) {
            throw createHttpError.Forbidden("You are not a member of this conversation");
        }
        const messages = await this.messageRepo.getContextMessages(conversationId, messageId, limit);

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

        const ONE_HOUR = 60 * 60 * 1000;
        if (message.created_at && Date.now() - new Date(message.created_at).getTime() > ONE_HOUR) {
            throw createHttpError.Forbidden("Chỉ được sửa tin nhắn trong vòng 1 tiếng kể từ lúc gửi");
        }

        const updatedAt = new Date();
        const updatedMessage = await this.messageRepo.updateContent(messageId, newContent, updatedAt);

        // Update recent message cache
        const convIdStr = message.conversation_id.toString();
        await this.messageCache.updateRecent(convIdStr, messageId, (m) => ({
            ...m,
            content: newContent,
            updated_at: updatedAt
        }));

        socketManager.emitToGroup(convIdStr, "message_edited", {
            messageId,
            content: newContent,
            updated_at: updatedAt,
            conversation_id: convIdStr,
            edit_history: updatedMessage?.edit_history
        });

        if (updatedMessage) {
            triggerSync('messages', SyncOperation.UPDATE, updatedMessage);
        }
        return updatedMessage;
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

        const updatedMessage = await this.messageRepo.updateStatus(messageId, 'recalled');

        // Update recent message cache
        const convIdStr = message.conversation_id?.toString() as string;
        await this.messageCache.updateRecent(convIdStr, messageId, (m) => ({
            ...m,
            status: 'recalled'
        }));

        socketManager.emitToGroup(convIdStr, "message_recalled", { messageId, conversation_id: convIdStr });
        return updatedMessage;
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

        // Push to dynamic message cache
        const formattedSaved = {
            ...savedMessage,
            sender: { id: 'system', name: 'System' }
        };
        await this.messageCache.pushRecent(conversationId, formattedSaved);

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

            // Update recent message cache
            await this.messageCache.updateRecent(convIdStr, updatedMessage.id!, (m) => ({
                ...m,
                status: 'sent',
                video: updatedMessage.video
            }));

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

        const convIdStr = message.conversation_id.toString();
        // Update recent message cache
        await this.messageCache.updateRecent(convIdStr, messageId, (m) => ({
            ...m,
            reactions: updatedMessage.reactions
        }));

        // Emit socket event to room (O(1))
        socketManager.emitToGroup(convIdStr, "message_reaction_updated", {
            message_id: updatedMessage.id,
            reactions: updatedMessage.reactions,
            conversation_id: convIdStr
        });

        return updatedMessage;
    }

    /**
     * Cập nhật watermark (đã nhận / đã đọc) trực tiếp vào Database (MongoDB), đồng bộ cache và phát Socket vào nhóm.
     * Không qua BullMQ để đảm bảo dữ liệu nhất quán ngay lập tức và client nhận ACK an toàn.
     */
    async updateWatermark(
        userId: string,
        payload: { conversationId: string; messageId: string; type: 'delivered' | 'read' }
    ) {
        const { conversationId, messageId, type } = payload;

        // 1. Cập nhật trực tiếp vào MongoDB thông qua ConversationFacade (đồng thời update ConversationCache)
        const updatedConv = await this.conversationFacade.updateWatermark(conversationId, userId, messageId, type);
        if (!updatedConv) {
            throw createHttpError.NotFound("Không tìm thấy cuộc trò chuyện để cập nhật watermark");
        }

        // 2. Cập nhật Redis watermark hash (O(1))
        await this.messageCache.updateWatermark(conversationId, userId, messageId, type);

        // 3. Broadcast sự kiện watermark_updated vào room (O(1))
        socketManager.emitToGroup(conversationId, SocketEvents.WATERMARK_UPDATED, {
            conversationId,
            userId,
            messageId,
            type
        });

        return {
            conversationId,
            userId,
            messageId,
            type
        };
    }
}
