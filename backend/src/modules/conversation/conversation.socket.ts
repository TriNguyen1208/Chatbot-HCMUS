import type { Socket } from "socket.io";
import type { SocketManager } from "#@/infrastructure/websocket/socket.manager.js";
import { CONVERSATION_ROOM } from "#@/infrastructure/websocket/socket.manager.js";
import { SocketEvents } from "#@/infrastructure/websocket/socket.events.js";
import type { SocketAckResponse } from "#@/infrastructure/websocket/socket.types.js";
import { validateSocketPayload } from "#@/infrastructure/websocket/socket.util.js";
import {
    CreateConversationSocketSchema,
    UpdateConversationSocketSchema,
    AddMembersSocketSchema,
    RemoveMembersSocketSchema,
    AssignAdminsSocketSchema,
    ConversationIdOnlySocketSchema
} from "./conversation.dto.js";
import type { Conversation } from "./conversation.entity.js";
import { conversationContainer } from "./conversation.container.js";
import { conversationFacade } from "./conversation.facade.js";

/**
 * Quản lý tất cả các sự kiện Socket liên quan đến Conversation:
 * - Tự động join socket vào tất cả phòng trò chuyện khi online
 * - Tạo nhóm / Tạo đoạn chat 1-1
 * - Cập nhật thông tin nhóm (Tên, avatar)
 * - Thêm / xóa thành viên, cập nhật admin
 * - Rời nhóm, chặn / bỏ chặn, giải tán nhóm
 */
export const registerConversationSocket = (socket: Socket, socketManager: SocketManager): void => {
    const userId = socket.data.userId as string;
    if (!userId) return;

    // 1. Tự động join socket vào tất cả các conversation rooms của user
    conversationFacade.getUserConversationIds(userId).then((convIds) => {
        const rooms = convIds.map(id => CONVERSATION_ROOM(id));
        if (rooms.length > 0) {
            socket.join(rooms);
            console.log(`[Socket.IO] Socket '${socket.id}' of user '${userId}' joined ${rooms.length} conversation room(s)`);
        }
    }).catch(err => {
        console.error(`[Socket.IO] Error joining conversation rooms for user '${userId}':`, err);
    });

    // 2. Tạo cuộc trò chuyện mới (Group hoặc 1-1)
    socket.on(SocketEvents.NEW_CONVERSATION, async (rawData: unknown, ack?: (res: SocketAckResponse<Conversation>) => void) => {
        try {
            const data = validateSocketPayload(CreateConversationSocketSchema, rawData, ack);
            if (!data) return;

            const created = await conversationContainer.conversationService.createConversation(userId, {
                ...data,
                primary_icon: data.primary_icon || '👍'
            });
            ack?.({
                success: true,
                data: created
            });
        } catch (error: any) {
            console.error(`[ConversationSocket] Error creating conversation for user ${userId}:`, error);
            ack?.({
                success: false,
                code: error.status || error.statusCode || 500,
                message: error.message || "Tạo cuộc trò chuyện thất bại"
            });
        }
    });

    // 3. Cập nhật thông tin nhóm (Tên, Avatar)
    socket.on(SocketEvents.UPDATE_CONVERSATION, async (rawData: unknown, ack?: (res: SocketAckResponse<Conversation>) => void) => {
        try {
            const data = validateSocketPayload(UpdateConversationSocketSchema, rawData, ack);
            if (!data) return;

            const updated = await conversationContainer.conversationService.updateConversation(userId, data.conversation_id, {
                name: data.name,
                avatar_url: data.avatar_url,
                primary_icon: data.primary_icon
            });
            ack?.({ success: true, data: updated || undefined });
        } catch (error: any) {
            console.error(`[ConversationSocket] Error updating conversation:`, error);
            ack?.({ success: false, code: error.status || error.statusCode || 500, message: error.message || "Cập nhật nhóm thất bại" });
        }
    });

    // 4. Thêm thành viên vào nhóm
    socket.on(SocketEvents.ADD_MEMBER_CONVERSATION, async (rawData: unknown, ack?: (res: SocketAckResponse<Conversation>) => void) => {
        try {
            const data = validateSocketPayload(AddMembersSocketSchema, rawData, ack);
            if (!data) return;

            const updated = await conversationContainer.conversationService.addMember(userId, data.conversation_id, data.member_ids);
            ack?.({ success: true, data: updated });
        } catch (error: any) {
            console.error(`[ConversationSocket] Error adding members:`, error);
            ack?.({ success: false, code: error.status || error.statusCode || 500, message: error.message || "Thêm thành viên thất bại" });
        }
    });

    // 5. Xóa / kick thành viên khỏi nhóm
    socket.on(SocketEvents.KICK_MEMBER_CONVERSATION, async (rawData: unknown, ack?: (res: SocketAckResponse<Conversation>) => void) => {
        try {
            const data = validateSocketPayload(RemoveMembersSocketSchema, rawData, ack);
            if (!data) return;

            const updated = await conversationContainer.conversationService.removeMembers(userId, data.conversation_id, data.member_ids);
            ack?.({ success: true, data: updated });
        } catch (error: any) {
            console.error(`[ConversationSocket] Error removing members:`, error);
            ack?.({ success: false, code: error.status || error.statusCode || 500, message: error.message || "Xóa thành viên thất bại" });
        }
    });

    // 6. Bổ nhiệm / giáng chức admin
    socket.on(SocketEvents.UPDATE_ADMIN, async (rawData: unknown, ack?: (res: SocketAckResponse<Conversation>) => void) => {
        try {
            const data = validateSocketPayload(AssignAdminsSocketSchema, rawData, ack);
            if (!data) return;

            const updated = await conversationContainer.conversationService.assignAdmins(userId, data.conversation_id, data.admin_ids);
            ack?.({ success: true, data: updated });
        } catch (error: any) {
            console.error(`[ConversationSocket] Error assigning admins:`, error);
            ack?.({ success: false, code: error.status || error.statusCode || 500, message: error.message || "Cập nhật admin thất bại" });
        }
    });

    // 7. Rời nhóm
    socket.on(SocketEvents.MEMBER_LEFT, async (rawData: unknown, ack?: (res: SocketAckResponse<void>) => void) => {
        try {
            const data = validateSocketPayload(ConversationIdOnlySocketSchema, rawData, ack);
            if (!data) return;

            await conversationContainer.conversationService.leaveGroup(userId, data.conversation_id);
            ack?.({ success: true });
        } catch (error: any) {
            console.error(`[ConversationSocket] Error leaving group:`, error);
            ack?.({ success: false, code: error.status || error.statusCode || 500, message: error.message || "Rời nhóm thất bại" });
        }
    });

    // 8. Chặn cuộc trò chuyện
    socket.on(SocketEvents.BLOCK_CONVERSATION, async (rawData: unknown, ack?: (res: SocketAckResponse<Conversation>) => void) => {
        try {
            const data = validateSocketPayload(ConversationIdOnlySocketSchema, rawData, ack);
            if (!data) return;

            const updated = await conversationContainer.conversationService.blockConversation(userId, data.conversation_id);
            ack?.({ success: true, data: updated || undefined });
        } catch (error: any) {
            console.error(`[ConversationSocket] Error blocking conversation:`, error);
            ack?.({ success: false, code: error.status || error.statusCode || 500, message: error.message || "Chặn cuộc trò chuyện thất bại" });
        }
    });

    // 9. Bỏ chặn cuộc trò chuyện
    socket.on(SocketEvents.UNBLOCK_CONVERSATION, async (rawData: unknown, ack?: (res: SocketAckResponse<Conversation>) => void) => {
        try {
            const data = validateSocketPayload(ConversationIdOnlySocketSchema, rawData, ack);
            if (!data) return;

            const updated = await conversationContainer.conversationService.unblockConversation(userId, data.conversation_id);
            ack?.({ success: true, data: updated || undefined });
        } catch (error: any) {
            console.error(`[ConversationSocket] Error unblocking conversation:`, error);
            ack?.({ success: false, code: error.status || error.statusCode || 500, message: error.message || "Bỏ chặn cuộc trò chuyện thất bại" });
        }
    });

    // 10. Giải tán nhóm
    socket.on(SocketEvents.DISBAND_GROUP, async (rawData: unknown, ack?: (res: SocketAckResponse<void>) => void) => {
        try {
            const data = validateSocketPayload(ConversationIdOnlySocketSchema, rawData, ack);
            if (!data) return;

            await conversationContainer.conversationService.disbandGroup(userId, data.conversation_id);
            ack?.({ success: true });
        } catch (error: any) {
            console.error(`[ConversationSocket] Error disbanding group:`, error);
            ack?.({ success: false, code: error.status || error.statusCode || 500, message: error.message || "Giải tán nhóm thất bại" });
        }
    });
};


