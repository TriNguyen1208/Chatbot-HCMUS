import type { Conversation, ConversationDB } from "../entities/conversation.entity.js";
import type { IDatabase } from "#@/infrastructure/database/database.interface.js";
import { Types } from "mongoose";

export interface IConversationRepository {
    create(data: Partial<ConversationDB>): Promise<Conversation>;

    findByID(id: string): Promise<Conversation | null>;

    checkUserInConversation(conversationId: string, userId: string): Promise<boolean>;

    getConversationsByUser(userId: string, limit?: number, cursorId?: string, type?: 'utu' | 'group'): Promise<Conversation[]>;

    findSelfConversation(userId: string): Promise<Conversation | null>;

    findDirectConversation(user1: string, user2: string): Promise<Conversation | null>;

    addMembers(conversationId: string, userIds: string[]): Promise<Conversation>;

    removeMember(conversationId: string, userId: string): Promise<void>;

    updateLastMessage(conversationId: string, messageId: string): Promise<Conversation>;

    removeMembers(conversationId: string, userIds: string[]): Promise<void>;

    addAdmins(conversationId: string, adminIds: string[]): Promise<Conversation>;

    updateWatermark(conversationId: string, userId: string, messageId: string, type: 'delivered' | 'read'): Promise<void>;
}

export class ConversationRepository implements IConversationRepository {
    constructor(
        private readonly db: IDatabase
    ) { }

    private formatConversation(doc: any): Conversation | null {
        if (!doc) return null;
        const { _id, __v, last_message_id, ...rest } = doc;

        let last_message = last_message_id;
        if (last_message_id && typeof last_message_id === 'object' && '_id' in last_message_id) {
            const { _id: msgId, __v: msgV, ...msgRest } = last_message_id;
            last_message = {
                id: msgId?.toString(),
                ...msgRest
            };
        }

        return {
            id: _id?.toString(),
            last_message,
            ...rest
        } as Conversation;
    }

    private getPopulateOptions() {
        return [
            { path: 'last_message_id' }
        ];
    }

    async create(data: Partial<ConversationDB>): Promise<Conversation> {
        const result = await this.db.insert<ConversationDB>('conversations', data, { populate: this.getPopulateOptions() });
        if (!result) throw new Error("Creating conversation failed");
        return this.formatConversation(result)!;
    }

    async findByID(id: string): Promise<Conversation | null> {
        if (!Types.ObjectId.isValid(id)) return null;
        const conversation = await this.db.findOne<ConversationDB>('conversations', { _id: new Types.ObjectId(id) }, { populate: this.getPopulateOptions() });
        if (!conversation) return null;

        return this.formatConversation(conversation);
    }

    async checkUserInConversation(conversationId: string, userId: string): Promise<boolean> {
        if (!Types.ObjectId.isValid(conversationId)) return false;
        const conv = await this.db.findOne<ConversationDB>('conversations', { _id: new Types.ObjectId(conversationId) });
        if (!conv) return false;
        const memberIds = conv.member_ids?.map((id) => id.toString()) || [];
        return memberIds.includes(userId);
    }

    async getConversationsByUser(userId: string, limit: number = 20, cursorId?: string, type?: 'utu' | 'group'): Promise<Conversation[]> {
        const conditions: any = { member_ids: userId, last_message_id: { $ne: null } };

        if (type) {
            conditions.type = type;
        }

        if (cursorId) {
            conditions.last_message_id = { $lt: cursorId };
        }

        const conversations = await this.db.query<ConversationDB>('conversations', conditions, {
            limit,
            orderBy: { field: 'last_message_id', ascending: false },
            populate: this.getPopulateOptions()
        });

        return conversations.map(c => this.formatConversation(c)).filter(Boolean) as Conversation[];
    }

    async findSelfConversation(userId: string): Promise<Conversation | null> {
        const conditions: Record<string, unknown> = {
            type: 'self',
            member_ids: { $all: [userId], $size: 1 }
        };
        const conv = await this.db.findOne<ConversationDB>('conversations', conditions, { populate: this.getPopulateOptions() });
        return this.formatConversation(conv);
    }

    async findDirectConversation(user1: string, user2: string): Promise<Conversation | null> {
        const conditions: Record<string, unknown> = {
            type: 'utu',
            member_ids: { $all: [user1, user2], $size: 2 }
        };
        const conv = await this.db.findOne<ConversationDB>('conversations', conditions, { populate: this.getPopulateOptions() });
        return this.formatConversation(conv);
    }

    async addMembers(conversationId: string, userIds: string[]): Promise<Conversation> {
        if (!Types.ObjectId.isValid(conversationId)) throw new Error("Invalid conversation ID");
        
        const updatedConv = await this.db.update('conversations', { _id: new Types.ObjectId(conversationId) }, { $addToSet: { member_ids: { $each: userIds.map(id => new Types.ObjectId(id)) } } }, { populate: this.getPopulateOptions() });
        return this.formatConversation(updatedConv)!;
    }

    async removeMember(conversationId: string, userId: string): Promise<void> {
        if (!Types.ObjectId.isValid(conversationId)) throw new Error("Invalid conversation ID");

        await this.db.update('conversations', { _id: new Types.ObjectId(conversationId) }, {
            $pull: {
                member_ids: new Types.ObjectId(userId),
                admin_ids: new Types.ObjectId(userId)
            }
        });
    }

    async removeMembers(conversationId: string, userIds: string[]): Promise<void> {
        if (!Types.ObjectId.isValid(conversationId)) throw new Error("Invalid conversation ID");

        const objectIds = userIds.map(id => new Types.ObjectId(id));
        await this.db.update('conversations', { _id: new Types.ObjectId(conversationId) }, {
            $pullAll: {
                member_ids: objectIds,
                admin_ids: objectIds
            }
        });
    }

    async addAdmins(conversationId: string, adminIds: string[]): Promise<Conversation> {
        if (!Types.ObjectId.isValid(conversationId)) throw new Error("Invalid conversation ID");

        const updatedConv = await this.db.update('conversations', { _id: new Types.ObjectId(conversationId) }, {
            $addToSet: {
                admin_ids: { $each: adminIds.map(id => new Types.ObjectId(id)) }
            }
        }, { populate: this.getPopulateOptions() });
        return this.formatConversation(updatedConv)!;
    }

    async updateLastMessage(conversationId: string, messageId: string): Promise<Conversation> {
        if (!Types.ObjectId.isValid(conversationId) || !Types.ObjectId.isValid(messageId)) throw new Error("Invalid conversation ID or message ID");
        const updatedConv = await this.db.update<ConversationDB>('conversations', { _id: new Types.ObjectId(conversationId) }, { last_message_id: new Types.ObjectId(messageId) }, { populate: this.getPopulateOptions() });
        return this.formatConversation(updatedConv)!;
    }

    async updateWatermark(conversationId: string, userId: string, messageId: string, type: 'delivered' | 'read'): Promise<void> {
        if (!Types.ObjectId.isValid(conversationId) || !Types.ObjectId.isValid(userId) || !Types.ObjectId.isValid(messageId)) return;
        
        const convId = new Types.ObjectId(conversationId);
        const uId = new Types.ObjectId(userId);
        const msgId = new Types.ObjectId(messageId);

        // Check if user exists in watermarks array
        const conv = await this.db.findOne<ConversationDB>('conversations', { 
            _id: convId, 
            "watermarks.user_id": uId 
        });

        if (!conv) {
            // Push new watermark
            const newWatermark: any = { user_id: uId };
            if (type === 'delivered') newWatermark.last_delivered_msg_id = msgId;
            if (type === 'read') newWatermark.last_read_msg_id = msgId;
            
            await this.db.update<ConversationDB>('conversations', 
                { _id: convId }, 
                { $push: { watermarks: newWatermark } }
            );
        } else {
            // Update existing watermark using arrayFilters
            const fieldToUpdate = type === 'delivered' ? "watermarks.$[elem].last_delivered_msg_id" : "watermarks.$[elem].last_read_msg_id";
            await this.db.update<ConversationDB>('conversations',
                { _id: convId },
                { $set: { [fieldToUpdate]: msgId } },
                { arrayFilters: [{ "elem.user_id": uId }] }
            );
        }
    }
}
