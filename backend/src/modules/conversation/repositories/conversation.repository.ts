import type { Conversation, ConversationDB, BlockInfo } from "../entities/conversation.entity.js";
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

    updateConversation(id: string, data: Partial<ConversationDB>): Promise<Conversation | null>;

    addAdmins(conversationId: string, adminIds: string[]): Promise<Conversation>;

    updateWatermark(conversationId: string, userId: string, messageId: string, type: 'delivered' | 'read'): Promise<void>;

    updateBlockStatus(conversationId: string, block: BlockInfo | null): Promise<Conversation | null>;
}

export class ConversationRepository implements IConversationRepository {
    constructor(
        private readonly db: IDatabase
    ) { }

    private formatConversation(doc: any): Conversation | null {
        if (!doc) return null;
        const { _id, __v, last_message_id, block, watermarks, ...rest } = doc;

        let last_message = last_message_id;
        if (last_message_id && typeof last_message_id === 'object' && '_id' in last_message_id) {
            const msgObj = typeof last_message_id.toObject === 'function' ? last_message_id.toObject() : last_message_id;
            const { _id: msgId, __v: msgV, ...msgRest } = msgObj;
            last_message = {
                id: msgId?.toString(),
                ...msgRest
            };
        }

        let cleanWatermarks: any[] = [];
        if (Array.isArray(watermarks)) {
            const map = new Map<string, any>();
            for (const wm of watermarks) {
                if (!wm || !wm.user_id) continue;
                const uid = wm.user_id.toString();
                const prev = map.get(uid);
                if (!prev) {
                    map.set(uid, {
                        user_id: uid,
                        last_delivered_msg_id: wm.last_delivered_msg_id ? wm.last_delivered_msg_id.toString() : null,
                        last_read_msg_id: wm.last_read_msg_id ? wm.last_read_msg_id.toString() : null,
                    });
                } else {
                    map.set(uid, {
                        user_id: uid,
                        last_delivered_msg_id: wm.last_delivered_msg_id ? wm.last_delivered_msg_id.toString() : prev.last_delivered_msg_id,
                        last_read_msg_id: wm.last_read_msg_id ? wm.last_read_msg_id.toString() : prev.last_read_msg_id,
                    });
                }
            }
            cleanWatermarks = Array.from(map.values());
        }

        return {
            id: _id?.toString(),
            last_message,
            watermarks: cleanWatermarks,
            block: block ? {
                block_by: block.block_by?.toString() || block.block_by,
                block_at: block.block_at
            } : null,
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
        if (!conv || conv.is_active === false) return false;
        const memberIds = conv.member_ids?.map((id) => id.toString()) || [];
        return memberIds.includes(userId);
    }

    async getConversationsByUser(userId: string, limit: number = 20, cursorId?: string, type?: 'utu' | 'group'): Promise<Conversation[]> {
        const conditions: any = { 
            member_ids: userId, 
            last_message_id: { $ne: null },
            is_active: { $ne: false }
        };

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
            member_ids: { $all: [userId], $size: 1 },
            is_active: { $ne: false }
        };
        const conv = await this.db.findOne<ConversationDB>('conversations', conditions, { populate: this.getPopulateOptions() });
        return this.formatConversation(conv);
    }

    async findDirectConversation(user1: string, user2: string): Promise<Conversation | null> {
        const conditions: Record<string, unknown> = {
            type: 'utu',
            member_ids: { $all: [user1, user2], $size: 2 },
            is_active: { $ne: false }
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

        const conv = await this.db.findOne<ConversationDB>('conversations', { _id: convId });
        if (!conv) return;

        const currentWatermarks = conv.watermarks || [];
        const map = new Map<string, any>();

        for (const wm of currentWatermarks) {
            if (!wm || !wm.user_id) continue;
            const uid = wm.user_id.toString();
            const prev = map.get(uid);
            if (!prev) {
                map.set(uid, {
                    user_id: new Types.ObjectId(uid),
                    last_delivered_msg_id: wm.last_delivered_msg_id ? new Types.ObjectId(wm.last_delivered_msg_id.toString()) : null,
                    last_read_msg_id: wm.last_read_msg_id ? new Types.ObjectId(wm.last_read_msg_id.toString()) : null,
                });
            } else {
                map.set(uid, {
                    user_id: new Types.ObjectId(uid),
                    last_delivered_msg_id: wm.last_delivered_msg_id ? new Types.ObjectId(wm.last_delivered_msg_id.toString()) : prev.last_delivered_msg_id,
                    last_read_msg_id: wm.last_read_msg_id ? new Types.ObjectId(wm.last_read_msg_id.toString()) : prev.last_read_msg_id,
                });
            }
        }

        const targetUid = uId.toString();
        const existing = map.get(targetUid);
        map.set(targetUid, {
            user_id: uId,
            last_delivered_msg_id: type === 'delivered' ? msgId : (existing?.last_delivered_msg_id || null),
            last_read_msg_id: type === 'read' ? msgId : (existing?.last_read_msg_id || null),
        });

        const newWatermarks = Array.from(map.values());
        await this.db.update<ConversationDB>('conversations',
            { _id: convId },
            { $set: { watermarks: newWatermarks } }
        );
    }

    async updateConversation(id: string, data: Partial<ConversationDB>): Promise<Conversation | null> {
        if (!Types.ObjectId.isValid(id)) throw new Error("Invalid conversation ID");
        const updated = await this.db.update<ConversationDB>(
            'conversations',
            { _id: new Types.ObjectId(id) },
            { $set: data },
            { populate: this.getPopulateOptions() }
        );
        console.log("Repo", this.formatConversation(updated))
        return this.formatConversation(updated);
    }

    async updateBlockStatus(conversationId: string, block: BlockInfo | null): Promise<Conversation | null> {
        if (!Types.ObjectId.isValid(conversationId)) throw new Error("Invalid conversation ID");
        const blockToSave = block ? {
            block_by: new Types.ObjectId(block.block_by.toString()),
            block_at: block.block_at || new Date()
        } : null;

        const updated = await this.db.update<ConversationDB>(
            'conversations',
            { _id: new Types.ObjectId(conversationId) },
            { $set: { block: blockToSave } as any },
            { populate: this.getPopulateOptions() }
        );
        return this.formatConversation(updated);
    }
}
