import { type IConversationRepository } from "./conversation.repository.js";
import type { CreateConversationDto, UpdateConversationDto } from "./conversation.dto.js";
import type { Conversation, ConversationDB } from "./conversation.entity.js";
import createError from "http-errors";
import { socketManager } from "#@/infrastructure/websocket/socket.manager.js";
import { MessageFacade } from "#@/modules/message/message.facade.js";
import { userFacade } from "#@/modules/user/user.facade.js";
import { triggerSync, SyncOperation } from "#@/shared/utils/sync.util.js";
import { ConversationCache } from "./conversation.cache.js";

export class ConversationService {
    constructor(
        private readonly conversationRepo: IConversationRepository,
        private readonly conversationCache: ConversationCache,
        private readonly messageFacade: MessageFacade
    ) { }

    /**
     * Creates a new conversation. If it's a 1-on-1 (utu) conversation that already exists,
     * returns the existing one. Otherwise, creates a new record and joins members to the socket room.
     * @param userId The ID of the user creating the conversation.
     * @param data The conversation data (type, members, etc.).
     * @returns The created or existing conversation.
     */
    async createConversation(userId: string, data: CreateConversationDto): Promise<Conversation> {
        const members = new Set([...data.member_ids, userId]);

        if (data.type === 'utu' && members.size !== 2) {
            throw createError(400, "A 1-1 conversation must have exactly 2 members");
        }
        if (data.type === 'self' && members.size !== 1) {
            throw createError(400, "A self conversation must have exactly 1 member");
        }
        // If utu or self, check if already exists
        if (data.type === 'utu') {
            const arr = Array.from(members);
            const cachedId = await this.conversationCache.getDirectConvId(arr[0]!, arr[1]!);
            if (cachedId) {
                try {
                    return await this.getConversationById(cachedId, userId);
                } catch {
                    // Fallback to database query if cache entry was stale/invalid
                }
            }
            const existing = await this.conversationRepo.findDirectConversation(arr[0]!, arr[1]!);
            if (existing) {
                if (existing.id) {
                    await this.conversationCache.setDirectConvId(arr[0]!, arr[1]!, existing.id);
                    await this.conversationCache.setConversation(existing.id, existing);
                }
                return existing;
            }
        } else if (data.type === 'self') {
            const cachedId = await this.conversationCache.getSelfConvId(userId);
            if (cachedId) {
                try {
                    return await this.getConversationById(cachedId, userId);
                } catch {
                    // Fallback to database query if cache entry was stale/invalid
                }
            }
            const existing = await this.conversationRepo.findSelfConversation(userId);
            if (existing) {
                if (existing.id) {
                    await this.conversationCache.setSelfConvId(userId, existing.id);
                    await this.conversationCache.setConversation(existing.id, existing);
                }
                return existing;
            }
        } else if (data.type === 'group' && !data.avatar_url) {
            // Assign creator's avatar if no avatar_url is provided
            const creator = await userFacade.findByID(userId);
            if (creator && creator.avatar_url) {
                data.avatar_url = creator.avatar_url;
            }
        }
        
        const newConversation: Partial<ConversationDB> = {
            ...data,
            member_ids: Array.from(members),
            admin_ids: data.type === 'group' ? [userId] : [],
            created_at: new Date(),
            is_active: true
        };
        const created = await this.conversationRepo.create(newConversation);

        // Populate Redis Cache (Full Conversation, UserConvs, Direct/Self mapping)
        if (created.id) {
            await this.conversationCache.setConversation(created.id, created);
            const memberIds = created.member_ids?.map((member_id) => member_id.toString()) || [];
            for (const mid of memberIds) {
                await this.conversationCache.addUserConv(mid, created.id);
            }
            if (created.type === 'utu') {
                const arr = Array.from(members);
                await this.conversationCache.setDirectConvId(arr[0]!, arr[1]!, created.id);
            } else if (created.type === 'self') {
                await this.conversationCache.setSelfConvId(userId, created.id);
            }
        }

        // Force all members to join the new room via SocketManager
        const new_members = created.member_ids?.map((member_id) => member_id.toString()) || [];
        if (created.id) {
            socketManager.joinGroup(new_members, created.id);
            socketManager.emitToGroup(created.id, "new_conversation", created);
        }

        if (data.type === 'group') {
            await this.sendSystemMessage(created.id!, "Nhóm đã được tạo");
        }

        triggerSync('conversations', SyncOperation.CREATE, created);

        return created;
    }

    async findOrCreateSelfConversation(userId: string): Promise<Conversation> {
        const cachedId = await this.conversationCache.getSelfConvId(userId);
        if (cachedId) {
            try {
                return await this.getConversationById(cachedId, userId);
            } catch {
                // Fallback to database query if cache entry was stale/invalid
            }
        }

        const existing = await this.conversationRepo.findSelfConversation(userId);
        if (existing) {
            if (existing.id) {
                await this.conversationCache.setSelfConvId(userId, existing.id);
                await this.conversationCache.setConversation(existing.id, existing);
            }
            return existing;
        }
        
        return this.createConversation(userId, {
            type: 'self',
            member_ids: [],
            primary_icon: 'default'
        });
    }

    /**
     * Retrieves a conversation by its ID and ensures the user has permission to view it.
     * @param conversationId The ID of the conversation.
     * @param userId The ID of the user attempting to access it.
     * @returns The conversation object.
     * @throws HttpError 404 if not found, 403 if the user is not a member.
     */
    async getConversationById(conversationId: string, userId: string): Promise<Conversation> {
        let conversation = await this.conversationCache.getConversation(conversationId);
        if (!conversation) {
            conversation = await this.conversationRepo.findByID(conversationId);
            if (conversation) {
                await this.conversationCache.setConversation(conversationId, conversation, 3600);
            }
        }

        if (!conversation || conversation.is_active === false) {
            throw createError(404, "Cuộc trò chuyện này đã bị giải tán hoặc không tồn tại");
        }
        
        const memberIds = conversation.member_ids?.map((id: any) => id.toString()) || [];
        if (!memberIds.includes(userId)) {
            throw createError(403, "You do not have permission to view this conversation");
        }

        return conversation;
    }

    /**
     * Retrieves all conversation IDs for a given user.
     * Caches in Redis Set user:{id}:convs.
     */
    async getUserConversationIds(userId: string): Promise<string[]> {
        const cachedConvs = await this.conversationCache.getUserConvs(userId);
        if (cachedConvs.length > 0) {
            return cachedConvs;
        }
        const convIds = await this.conversationRepo.getUserConversationIds(userId);
        if (convIds.length > 0) {
            await this.conversationCache.setUserConvs(userId, convIds);
        }
        return convIds;
    }

    /**
     * Retrieves member IDs of a conversation.
     * Checks cache first; falls back to repository if cache misses and sets cache.
     * If userId is provided, verifies that the user is a member.
     */
    async getConversationMembers(conversationId: string, userId?: string): Promise<string[]> {
        const cachedMembers = await this.conversationCache.getMembers(conversationId);
        if (cachedMembers.length > 0) {
            if (userId && !cachedMembers.includes(userId)) {
                return [];
            }
            return cachedMembers;
        }

        try {
            const conv = await this.conversationRepo.findByID(conversationId);
            if (!conv || conv.is_active === false) return [];
            await this.conversationCache.setConversation(conversationId, conv);

            const memberIds = conv?.member_ids?.map((id: any) => id.toString()) || [];
            if (userId && !memberIds.includes(userId)) {
                return [];
            }
            return memberIds;
        } catch {
            return [];
        }
    }

    /**
     * Retrieves a paginated list of conversations for a specific user.
     * @param userId The ID of the user.
     * @param limit The maximum number of conversations to return.
     * @param cursorId The ID of the last message used for cursor-based pagination.
     * @param type Optional filter by conversation type ('utu' or 'group').
     * @returns An array of conversations.
     */
    async getConversationList(userId: string, limit: number = 20, cursorId?: string, type?: 'utu' | 'group'): Promise<any[]> {
        const conversations = await this.conversationRepo.getConversationsByUser(userId, limit, cursorId, type);
        return conversations;
    }

    /**
     * Sends a system-generated message into a conversation.
     * @param conversationId The ID of the conversation.
     * @param text The system message content.
     */
    private async sendSystemMessage(conversationId: string, text: string) {
        await this.messageFacade.createSystemMessage(conversationId, text);
    }

    /**
     * Updates conversation info (name, avatar_url, primary_icon) for group,
     * or primary_icon for utu (1-on-1).
     */
    async updateConversation(userId: string, conversationId: string, data: UpdateConversationDto): Promise<any> {
        const conv = await this.getConversationById(conversationId, userId);
        
        let updatePayload: Partial<ConversationDB> = {};

        if (conv.type === 'utu') {
            if (data.primary_icon !== undefined) {
                updatePayload.primary_icon = data.primary_icon;
            }
        } else if (conv.type === 'group') {
            const adminIds = conv.admin_ids?.map((id: any) => id.toString()) || [];
            if (!adminIds.includes(userId)) throw createError(403, "Only admins can update group info");

            if (data.name !== undefined) updatePayload.name = data.name;
            if (data.avatar_url !== undefined) updatePayload.avatar_url = data.avatar_url;
            if (data.primary_icon !== undefined) updatePayload.primary_icon = data.primary_icon;
        } else {
            throw createError(400, "Only group and utu conversations can be updated");
        }

        if (Object.keys(updatePayload).length === 0) return conv;

        const updatedConv = await this.conversationRepo.updateConversation(conversationId, updatePayload);
        if (!updatedConv) throw createError(500, "Failed to update conversation");

        // Update cache (single source of truth)
        await this.conversationCache.setConversation(conversationId, updatedConv);

        triggerSync('conversations', SyncOperation.UPDATE, updatedConv);

        // Emit real-time socket event to room (O(1))
        socketManager.emitToGroup(conversationId, "conversation_updated", updatedConv);
        
        return updatedConv;
    }

    /**
     * Adds new members to a group conversation.
     * Validates admin permissions and handles socket room joins and notifications.
     * @param adminId The ID of the admin performing the action.
     * @param conversationId The ID of the group conversation.
     * @param newMemberIds An array of user IDs to add.
     * @throws HttpError 400 or 403 on invalid operations.
     */
    async addMember(adminId: string, conversationId: string, newMemberIds: string[]): Promise<Conversation> {
        const conv = await this.getConversationById(conversationId, adminId);
        if (conv.type !== 'group') throw createError(400, "Can only add members to a group");
        const adminIds = conv.admin_ids?.map((id: any) => id.toString()) || [];
        if (!adminIds.includes(adminId)) throw createError(403, "Only admins can add members");

        // Filter out members that are already in the group
        const currentMemberIds = conv.member_ids?.map((id: any) => id.toString()) || [];
        const membersToAdd = newMemberIds.filter(id => !currentMemberIds.includes(id));
        if (membersToAdd.length === 0) throw createError(400, "All users are already members");

        const updatedConv = await this.conversationRepo.addMembers(conversationId, membersToAdd);
        
        // Update cache
        if (updatedConv) {
            await this.conversationCache.setConversation(conversationId, updatedConv);
        }
        for (const mid of membersToAdd) {
            await this.conversationCache.addUserConv(mid, conversationId);
        }

        // Join new members into room
        socketManager.joinGroup(membersToAdd, conversationId);

        // Notify each new member with full conversation data
        for (const mid of membersToAdd) {
            socketManager.emitToUser(mid, "new_conversation", updatedConv);
        }

        // Broadcast to group room (O(1))
        socketManager.emitToGroup(conversationId, "members_added", { conversationId, newMemberIds: membersToAdd });

        const addedUsers = await userFacade.getBulk(membersToAdd);
        const names = addedUsers.map(u => u.name?.trim()).filter(Boolean);
        const nameStr = names.length > 0 ? names.join(", ") : `${membersToAdd.length} user(s)`;
        await this.sendSystemMessage(conversationId, `${nameStr} is added to group`);

        triggerSync('conversations', SyncOperation.UPDATE, updatedConv);
        return updatedConv;
    }

    /**
     * Removes multiple members from an existing group conversation.
     * Requires admin privileges and validates minimum remaining member count (>= 2).
     * @param adminId The ID of the admin performing the action.
     * @param conversationId The ID of the group conversation.
     * @param memberIds An array of member IDs to remove.
     * @throws HttpError 400 or 403 on invalid operations.
     */
    async removeMembers(adminId: string, conversationId: string, memberIds: string[]): Promise<Conversation> {
        const conv = await this.getConversationById(conversationId, adminId);
        if (conv.type !== 'group') throw createError(400, "Can only remove members from a group");
        const adminIdsSet = new Set(conv.admin_ids?.map((id: any) => id.toString()) || []);
        if (!adminIdsSet.has(adminId)) throw createError(403, "Only admins can remove members");

        // Filter out members that are not in the group, self, or admins
        const currentMemberIds = conv.member_ids?.map((id: any) => id.toString()) || [];
        const validMemberIds = memberIds.filter(id =>
            currentMemberIds.includes(id) &&
            !adminIdsSet.has(id)
        );

        if (validMemberIds.length === 0) {
            throw createError(400, "Không thể xóa Admin khỏi nhóm. Chỉ có thể xóa thành viên thường.");
        }

        const remainingCount = currentMemberIds.length - validMemberIds.length;
        if (remainingCount < 2) {
            throw createError(400, "Nhóm phải duy trì tối thiểu 2 thành viên");
        }

        const updatedConv = await this.conversationRepo.removeMembers(conversationId, validMemberIds);

        if (updatedConv) {
            await this.conversationCache.setConversation(conversationId, updatedConv);
            triggerSync('conversations', SyncOperation.UPDATE, updatedConv);
        }
        for (const mid of validMemberIds) {
            await this.conversationCache.removeUserConv(mid, conversationId);
        }

        // Thông báo cho cả phòng (kể cả người bị kick) trước khi rút khỏi phòng
        socketManager.emitToGroup(conversationId, "members_kicked", { conversationId, memberIds: validMemberIds });
        socketManager.leaveGroup(validMemberIds, conversationId);

        await this.sendSystemMessage(conversationId, `Admin đã xóa ${validMemberIds.length} thành viên khỏi nhóm`);
        return updatedConv;
    }

    /**
     * Assigns admin privileges to one or more members of a group.
     * Requires admin privileges.
     * @param adminId The ID of the admin performing the action.
     * @param conversationId The ID of the group conversation.
     * @param newAdminIds An array of member IDs to promote to admin.
     * @throws HttpError 400 or 403 on invalid operations.
     */
    async assignAdmins(adminId: string, conversationId: string, newAdminIds: string[]): Promise<Conversation> {
        const conv = await this.getConversationById(conversationId, adminId);
        if (conv.type !== 'group') throw createError(400, "Can only assign admins in a group");
        const adminIds = conv.admin_ids?.map((id: any) => id.toString()) || [];
        if (!adminIds.includes(adminId)) throw createError(403, "Only admins can assign admin status");

        const currentMemberIds = conv.member_ids?.map((id) => id.toString()) || [];
        const validAdminIds = newAdminIds.filter(id => currentMemberIds.includes(id));
        if (validAdminIds.length === 0) throw createError(400, "No valid group members selected to promote to admin");

        const updatedConv = await this.conversationRepo.addAdmins(conversationId, validAdminIds);

        if (updatedConv) {
            await this.conversationCache.setConversation(conversationId, updatedConv);
        }
        // Broadcast to group room (O(1))
        socketManager.emitToGroup(conversationId, "admins_updated", { conversationId, adminIds: validAdminIds });
        await this.sendSystemMessage(conversationId, `Admin đã cấp quyền Quản trị viên cho thành viên mới`);
        return updatedConv;
    }

    /**
     * Allows a user to leave a group conversation.
     * If the user is the sole admin, they must transfer admin rights before leaving.
     * @param userId The ID of the user leaving the group.
     * @param conversationId The ID of the group conversation.
     * @throws HttpError 400 on invalid operations.
     */
    async leaveGroup(userId: string, conversationId: string): Promise<Conversation> {
        const conv = await this.getConversationById(conversationId, userId);
        if (conv.type !== 'group') throw createError(400, "Can only leave a group");

        const adminIds = conv.admin_ids?.map((id) => id.toString()) || [];
        const memberIds = conv.member_ids?.map((id) => id.toString()) || [];
        
        const isAdmin = adminIds.includes(userId);
        const adminCount = adminIds.length;
        const memberCount = memberIds.length;

        if (isAdmin && adminCount <= 1 && memberCount > 1) {
            throw createError(400, "Bạn là Admin duy nhất. Vui lòng chuyển quyền Admin trước khi rời nhóm");
        }

        const userName = "Một thành viên";

        const updatedConv = await this.conversationRepo.removeMembers(conversationId, [userId]);

        await this.conversationCache.removeUserConv(userId, conversationId);

        if (updatedConv) {
            await this.conversationCache.setConversation(conversationId, updatedConv);
            triggerSync('conversations', SyncOperation.UPDATE, updatedConv);
        }

        // Broadcast member_left to room BEFORE leaving
        socketManager.emitToGroup(conversationId, "member_left", { conversationId, userId });
        socketManager.leaveGroup(userId, conversationId);

        await this.sendSystemMessage(conversationId, `${userName} đã rời khỏi nhóm`);
        return updatedConv;
    }

    /**
     * Updates the last message for a conversation and refreshes the cache.
     * @param conversationId The ID of the conversation
     * @param messageId The ID of the message
     */
    async updateLastMessage(conversationId: string, messageId: string): Promise<Conversation> {
        const updated = await this.conversationRepo.updateLastMessage(conversationId, messageId);
        if (updated) {
            await this.conversationCache.setConversation(conversationId, updated);
        }
        return updated;
    }

    /**
     * Updates the watermark (read/delivered status) for a user in a conversation.
     * @param conversationId The ID of the conversation
     * @param userId The ID of the user updating their watermark
     * @param messageId The ID of the message
     * @param type 'delivered' or 'read'
     */
    async updateWatermark(conversationId: string, userId: string, messageId: string, type: 'delivered' | 'read'): Promise<Conversation | null> {
        // Ensure conversation exists and user is a member
        await this.getConversationById(conversationId, userId);
        
        const updated = await this.conversationRepo.updateWatermark(conversationId, userId, messageId, type);
        if (updated) {
            await this.conversationCache.setConversation(conversationId, updated);
        }
        return updated;
    }

    /**
     * Blocks a 1-on-1 (utu) conversation.
     * @param userId The ID of the user requesting the block
     * @param conversationId The ID of the conversation
     */
    async blockConversation(userId: string, conversationId: string): Promise<Conversation> {
        const conv = await this.getConversationById(conversationId, userId);
        if (conv.type !== 'utu') {
            throw createError(400, "Chỉ có thể chặn cuộc trò chuyện 1-1");
        }

        if (conv.block) {
            if (conv.block.block_by?.toString() === userId) {
                throw createError(400, "Bạn đã chặn người dùng này rồi");
            } else {
                throw createError(400, "Cuộc trò chuyện đã bị đối phương chặn");
            }
        }

        const updated = await this.conversationRepo.updateBlockStatus(conversationId, {
            block_by: userId,
            block_at: new Date()
        });

        if (!updated) throw createError(500, "Không thể cập nhật trạng thái chặn");

        await this.conversationCache.setConversation(conversationId, updated);

        // Emit socket to room (O(1))
        socketManager.emitToGroup(conversationId, "conversation_blocked", updated);

        return updated;
    }

    /**
     * Unblocks a 1-on-1 (utu) conversation.
     * @param userId The ID of the user requesting the unblock
     * @param conversationId The ID of the conversation
     */
    async unblockConversation(userId: string, conversationId: string): Promise<Conversation> {
        const conv = await this.getConversationById(conversationId, userId);
        if (conv.type !== 'utu') {
            throw createError(400, "Chỉ có thể bỏ chặn cuộc trò chuyện 1-1");
        }

        if (!conv.block) {
            throw createError(400, "Cuộc trò chuyện chưa bị chặn");
        }

        if (conv.block.block_by?.toString() !== userId) {
            throw createError(403, "Bạn không có quyền bỏ chặn vì bạn không phải người đã chặn");
        }

        const updated = await this.conversationRepo.updateBlockStatus(conversationId, null);
        if (!updated) throw createError(500, "Không thể cập nhật trạng thái bỏ chặn");

        await this.conversationCache.setConversation(conversationId, updated);

        // Emit socket to room (O(1))
        socketManager.emitToGroup(conversationId, "conversation_unblocked", updated);

        return updated;
    }

    /**
     * Disbands a group conversation (soft-delete with is_active = false).
     * Only group admins can disband the group.
     * @param adminId The ID of the admin performing the action
     * @param conversationId The ID of the conversation
     */
    async disbandGroup(adminId: string, conversationId: string): Promise<Conversation | null> {
        const conv = await this.getConversationById(conversationId, adminId);
        if (conv.type !== 'group') {
            throw createError(400, "Chỉ có thể giải tán cuộc trò chuyện nhóm");
        }

        const adminIds = conv.admin_ids?.map((id: any) => id.toString()) || [];
        if (!adminIds.includes(adminId)) {
            throw createError(403, "Chỉ Quản trị viên mới có quyền giải tán nhóm");
        }

        const memberIds = conv.member_ids?.map((id: any) => id.toString()) || [];

        // 1. Update database: is_active = false (keep member_ids for audit/history)
        const updatedConv = await this.conversationRepo.updateConversation(conversationId, { is_active: false });

        // 2. Invalidate redis cache and remove from user conversations
        await this.conversationCache.invalidateConversation(conversationId);
        for (const mid of memberIds) {
            await this.conversationCache.removeUserConv(mid, conversationId);
        }

        // 3. Remove from Elasticsearch index
        triggerSync('conversations', SyncOperation.DELETE, { id: conversationId });

        // 4. Emit socket event to room BEFORE leaving
        socketManager.emitToGroup(conversationId, "group_disbanded", {
            conversationId,
            disbanded_by: adminId,
            group_name: conv.name
        });
        socketManager.leaveGroup(memberIds, conversationId);

        return updatedConv;
    }
}
