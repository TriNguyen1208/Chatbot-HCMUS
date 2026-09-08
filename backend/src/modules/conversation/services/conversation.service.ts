import { type IConversationRepository } from "../repositories/conversation.repository.js";
import type { CreateConversationDto } from "../dto/conversation.dto.js";
import type { Conversation, ConversationDB } from "../entities/conversation.entity.js";
import createError from "http-errors";
import { socketManager } from "#@/infrastructure/websocket/socket-manager.js";
import { MessageFacade } from "#@/modules/message/message.facade.js";
import { redisClient } from "#@/infrastructure/redis/redis.js";
import { userFacade } from "#@/modules/user/user.facade.js";
import { triggerSync } from "#@/utils/sync.util.js";
import { SyncOperation } from "#@/infrastructure/rabbitmq/types.js";

export class ConversationService {
    constructor(
        private readonly conversationRepo: IConversationRepository,
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
        // If utu, check if already exists
        if (data.type === 'utu') {
            const arr = Array.from(members);
            const existing = await this.conversationRepo.findDirectConversation(arr[0]!, arr[1]!);
            if (existing) return existing;
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

        // Force all members to join the new room via SocketManager
        const new_members = created.member_ids?.map((member_id) => member_id.toString()) || [];
        socketManager.emitToUsers(
            new_members,
            "new_conversation",
            created
        );

        if (data.type === 'group') {
            await this.sendSystemMessage(created.id!, "Nhóm đã được tạo");
        }

        triggerSync('conversations', SyncOperation.CREATE, created);

        return created;
    }

    /**
     * Retrieves a conversation by its ID and ensures the user has permission to view it.
     * @param conversationId The ID of the conversation.
     * @param userId The ID of the user attempting to access it.
     * @returns The conversation object.
     * @throws HttpError 404 if not found, 403 if the user is not a member.
     */
    async getConversationById(conversationId: string, userId: string): Promise<Conversation> {
        const cacheKey = `conversation:${conversationId}`;
        let conversation = await redisClient.getJSON(cacheKey);
        if (!conversation) {
            conversation = await this.conversationRepo.findByID(conversationId);
            if (conversation) {
                await redisClient.setJSON(cacheKey, conversation, 3600);
            }
        }

        if (!conversation) {
            throw createError(404, "This conversation was not found");
        }
        
        const memberIds = conversation.member_ids?.map((id: any) => id.toString()) || [];
        if (!memberIds.includes(userId)) {
            throw createError(403, "You do not have permission to view this conversation");
        }

        return conversation;
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
     * Adds new members to a group conversation.
     * Validates admin permissions and handles socket room joins and notifications.
     * @param adminId The ID of the admin performing the action.
     * @param conversationId The ID of the group conversation.
     * @param newMemberIds An array of user IDs to add.
     * @throws HttpError 400 or 403 on invalid operations.
     */
    async addMember(adminId: string, conversationId: string, newMemberIds: string[]) {
        const conv = await this.getConversationById(conversationId, adminId);
        if (conv.type !== 'group') throw createError(400, "Can only add members to a group");
        const adminIds = conv.admin_ids?.map((id: any) => id.toString()) || [];
        if (!adminIds.includes(adminId)) throw createError(403, "Only admins can add members");

        // Filter out members that are already in the group
        const currentMemberIds = conv.member_ids?.map((id: any) => id.toString()) || [];
        const membersToAdd = newMemberIds.filter(id => !currentMemberIds.includes(id));
        if (membersToAdd.length === 0) throw createError(400, "All users are already members");

        const updatedConv = await this.conversationRepo.addMembers(conversationId, membersToAdd);
        
        // Invalidate cache
        await redisClient.del(`conversation:${conversationId}`);

        const allMembers = updatedConv?.member_ids?.map((id) => id.toString()) || [];
        socketManager.emitToUsers(membersToAdd, "new_conversation", updatedConv);
        socketManager.emitToUsers(allMembers, "members_added", { conversationId, newMemberIds: membersToAdd });
        await this.sendSystemMessage(conversationId, `${membersToAdd.length} user(s) added to group`);

        triggerSync('conversations', SyncOperation.UPDATE, updatedConv);
    }

    /**
     * Removes multiple members from an existing group conversation.
     * Requires admin privileges and validates minimum remaining member count (>= 2).
     * @param adminId The ID of the admin performing the action.
     * @param conversationId The ID of the group conversation.
     * @param memberIds An array of member IDs to remove.
     * @throws HttpError 400 or 403 on invalid operations.
     */
    async removeMembers(adminId: string, conversationId: string, memberIds: string[]) {
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

        await this.conversationRepo.removeMembers(conversationId, validMemberIds);

        // Invalidate cache
        await redisClient.del(`conversation:${conversationId}`);

        const remainingMembers = conv.member_ids?.map((id) => id.toString()).filter(id => !validMemberIds.includes(id)) || [];
        // Thông báo cho cả người bị kick và người còn lại
        socketManager.emitToUsers([...remainingMembers, ...validMemberIds], "members_kicked", { conversationId, memberIds: validMemberIds });
        await this.sendSystemMessage(conversationId, `Admin đã xóa ${validMemberIds.length} thành viên khỏi nhóm`);

        const updatedConvForSync = await this.conversationRepo.findByID(conversationId);
        if (updatedConvForSync) {
            triggerSync('conversations', SyncOperation.UPDATE, updatedConvForSync);
        }
    }

    /**
     * Assigns admin privileges to one or more members of a group.
     * Requires admin privileges.
     * @param adminId The ID of the admin performing the action.
     * @param conversationId The ID of the group conversation.
     * @param newAdminIds An array of member IDs to promote to admin.
     * @throws HttpError 400 or 403 on invalid operations.
     */
    async assignAdmins(adminId: string, conversationId: string, newAdminIds: string[]) {
        const conv = await this.getConversationById(conversationId, adminId);
        if (conv.type !== 'group') throw createError(400, "Can only assign admins in a group");
        const adminIds = conv.admin_ids?.map((id: any) => id.toString()) || [];
        if (!adminIds.includes(adminId)) throw createError(403, "Only admins can assign admin status");

        const currentMemberIds = conv.member_ids?.map((id) => id.toString()) || [];
        const validAdminIds = newAdminIds.filter(id => currentMemberIds.includes(id));
        if (validAdminIds.length === 0) throw createError(400, "No valid group members selected to promote to admin");

        await this.conversationRepo.addAdmins(conversationId, validAdminIds);

        // Invalidate cache
        await redisClient.del(`conversation:${conversationId}`);
        socketManager.emitToUsers(currentMemberIds, "admins_updated", { conversationId, adminIds: validAdminIds });
        await this.sendSystemMessage(conversationId, `Admin đã cấp quyền Quản trị viên cho thành viên mới`);
    }

    /**
     * Allows a user to leave a group conversation.
     * If the user is the sole admin, they must transfer admin rights before leaving.
     * @param userId The ID of the user leaving the group.
     * @param conversationId The ID of the group conversation.
     * @throws HttpError 400 on invalid operations.
     */
    async leaveGroup(userId: string, conversationId: string) {
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

        await this.conversationRepo.removeMember(conversationId, userId);

        // Invalidate cache
        await redisClient.del(`conversation:${conversationId}`);

        socketManager.emitToUsers(memberIds, "member_left", { conversationId, userId });
        await this.sendSystemMessage(conversationId, `${userName} đã rời khỏi nhóm`);

        const updatedConvForSync = await this.conversationRepo.findByID(conversationId);
        if (updatedConvForSync) {
            triggerSync('conversations', SyncOperation.UPDATE, updatedConvForSync);
        }
    }

    /**
     * Updates the watermark (read/delivered status) for a user in a conversation.
     * @param conversationId The ID of the conversation
     * @param userId The ID of the user updating their watermark
     * @param messageId The ID of the message
     * @param type 'delivered' or 'read'
     */
    async updateWatermark(conversationId: string, userId: string, messageId: string, type: 'delivered' | 'read') {
        // Ensure conversation exists and user is a member
        await this.getConversationById(conversationId, userId);
        
        await this.conversationRepo.updateWatermark(conversationId, userId, messageId, type);
        
        // Invalidate cache so that next fetch gets the updated watermarks
        await redisClient.del(`conversation:${conversationId}`);
    }
}
