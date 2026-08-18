import type { Conversation } from "./entities/conversation.entity.js";
import { conversationContainer } from "./conversation.container.js";

export class ConversationFacade {
    private get conversationRepo() {
        return conversationContainer.conversationRepo;
    }

    private get conversationService() {
        return conversationContainer.conversationService;
    }

    async isUserInConversation(conversationId: string, userId: string): Promise<boolean> {
        try {
            await this.conversationService.getConversationById(conversationId, userId);
            return true;
        } catch {
            return false;
        }
    }

    async updateLastMessage(conversationId: string, messageId: string): Promise<void> {
        return await this.conversationRepo.updateLastMessage(conversationId, messageId);
    }

    async createConversation(
        userId: string, 
        data: { type: 'utu' | 'group', name?: string, member_ids: string[], primary_icon?: string }
    ): Promise<Conversation> {
        return await this.conversationService.createConversation(userId, {
            ...data,
            primary_icon: data.primary_icon || 'default'
        });
    }
}
export const conversationFacade = new ConversationFacade();