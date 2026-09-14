import type { Conversation } from "./conversation.entity.js";
import { conversationContainer } from "./conversation.container.js";

export class ConversationFacade {
    private get conversationService() {
        return conversationContainer.conversationService;
    }

    private get conversationCache() {
        return conversationContainer.conversationCache;
    }

    /**
     * Kiểm tra user có thuộc cuộc trò chuyện hay không.
     * Ưu tiên đọc từ cache conversation:{id} (0.1ms). Nếu miss mới fallback sang DB.
     */
    async isUserInConversation(conversationId: string, userId: string): Promise<boolean> {
        const cached = await this.conversationCache.isMember(conversationId, userId);
        if (cached) {
            return true;
        }

        try {
            await this.conversationService.getConversationById(conversationId, userId);
            return true;
        } catch {
            return false;
        }
    }

    /**
     * Lấy danh sách thành viên của cuộc trò chuyện.
     * Ưu tiên đọc từ cache conversation:{id} (0.1ms). Nếu miss mới fallback sang DB.
     * Nếu truyền userId: chỉ trả về nếu userId là thành viên.
     * Nếu không truyền userId: trả về toàn bộ thành viên.
     */
    async getConversationMembers(conversationId: string, userId?: string): Promise<string[]> {
        return this.conversationService.getConversationMembers(conversationId, userId);
    }

    /**
     * Lấy danh sách các conversation IDs của user (phục vụ join Socket room lúc online).
     * Ưu tiên đọc từ Redis Set user:{id}:convs.
     */
    async getUserConversationIds(userId: string): Promise<string[]> {
        return await this.conversationService.getUserConversationIds(userId);
    }

    async updateLastMessage(conversationId: string, messageId: string): Promise<Conversation> {
        return this.conversationService.updateLastMessage(conversationId, messageId);
    }

    async createConversation(
        userId: string, 
        data: { type: 'utu' | 'group' | 'self', name?: string, member_ids: string[], primary_icon?: string }
    ): Promise<Conversation> {
        const created = await this.conversationService.createConversation(userId, {
            ...data,
            primary_icon: data.primary_icon || 'default'
        });
        return created;
    }

    async findOrCreateSelfConversation(userId: string): Promise<Conversation> {
        const conv = await this.conversationService.findOrCreateSelfConversation(userId);
        return conv;
    }

    async updateWatermark(conversationId: string, userId: string, messageId: string, type: 'delivered' | 'read'): Promise<Conversation | null> {
        const updated = await this.conversationService.updateWatermark(conversationId, userId, messageId, type);
        return updated;
    }

    async getConversationById(conversationId: string, userId: string): Promise<Conversation> {
        const conv = await this.conversationService.getConversationById(conversationId, userId);
        return conv;
    }

    async updateConversation(userId: string, conversationId: string, data: any): Promise<Conversation> {
        const updated = await this.conversationService.updateConversation(userId, conversationId, data);
        return updated;
    }

    async addMember(adminId: string, conversationId: string, newMemberIds: string[]): Promise<Conversation> {
        const updated = await this.conversationService.addMember(adminId, conversationId, newMemberIds);
        return updated;
    }

    async removeMembers(adminId: string, conversationId: string, memberIds: string[]): Promise<Conversation> {
        const updated = await this.conversationService.removeMembers(adminId, conversationId, memberIds);
        return updated;
    }

    async assignAdmins(adminId: string, conversationId: string, newAdminIds: string[]): Promise<Conversation> {
        const updated = await this.conversationService.assignAdmins(adminId, conversationId, newAdminIds);
        return updated;
    }

    async leaveGroup(userId: string, conversationId: string): Promise<Conversation> {
        const updated = await this.conversationService.leaveGroup(userId, conversationId);
        return updated;
    }

    async blockConversation(userId: string, conversationId: string): Promise<Conversation> {
        const updated = await this.conversationService.blockConversation(userId, conversationId);
        return updated;
    }

    async unblockConversation(userId: string, conversationId: string): Promise<Conversation> {
        const updated = await this.conversationService.unblockConversation(userId, conversationId);
        return updated;
    }

    async disbandGroup(adminId: string, conversationId: string): Promise<Conversation | null> {
        return this.conversationService.disbandGroup(adminId, conversationId);
    }
}
export const conversationFacade = new ConversationFacade();