import type { Message, MessageDB, MessageModel } from "../entities/message.entity.js";
import type { IDatabase } from "#@/infrastructure/database/database.interface.js";
import { Types } from "mongoose";
import createHttpError from "http-errors";

export class MessageRepository {
    constructor(
        private readonly db: IDatabase
    ) {}


    async create(messageData: MessageDB): Promise<Message> {
        if (typeof messageData.conversation_id === 'string' && Types.ObjectId.isValid(messageData.conversation_id)) {
            messageData.conversation_id = new Types.ObjectId(messageData.conversation_id);
        }
        const result = await this.db.insert<MessageDB>('messages', messageData);
        const message = Array.isArray(result) ? result[0] : result;
        if(!message) {
            throw createHttpError[400];
        }
        return this.mapToDomain(message);
    }

    async findByID(id: string): Promise<Message | null> {
        if (!Types.ObjectId.isValid(id)) return null;
        const msg = await this.db.findOne<MessageDB>('messages', { _id: new Types.ObjectId(id) });
        if (!msg) return null;
        return this.mapToDomain(msg);
    }

    async getMessages(conversationId: string, limit: number = 20, cursorId?: string): Promise<Message[]> {
        const conditions: any = { conversation_id: conversationId, status: { $in: ['sent', 'recalled'] } };
        
        if (cursorId) {
            conditions._id = { $lt: new Types.ObjectId(cursorId) };
        }
        
        const messages = await this.db.query<MessageDB>('messages', conditions, {
            limit,
            orderBy: { field: '_id', ascending: false }
        });
        return messages.map(msg => this.mapToDomain(msg));
    }

    async getContextMessages(conversationId: string, messageId: string, limit: number = 10): Promise<Message[]> {
        if (!Types.ObjectId.isValid(messageId) || !Types.ObjectId.isValid(conversationId)) return [];
        
        const beforeMessages = await this.db.query<MessageDB>('messages', {
            conversation_id: new Types.ObjectId(conversationId),
            status: { $in: ['sent', 'recalled'] },
            _id: { $lt: new Types.ObjectId(messageId) }
        } as any, {
            limit,
            orderBy: { field: '_id', ascending: false }
        });

        const targetMessage = await this.db.findOne<MessageDB>('messages', {
            _id: new Types.ObjectId(messageId),
            conversation_id: new Types.ObjectId(conversationId),
            status: { $in: ['sent', 'recalled'] }
        } as any);

        const afterMessages = await this.db.query<MessageDB>('messages', {
            conversation_id: new Types.ObjectId(conversationId),
            status: { $in: ['sent', 'recalled'] },
            _id: { $gt: new Types.ObjectId(messageId) }
        } as any, {
            limit,
            orderBy: { field: '_id', ascending: true }
        });

        const result: MessageDB[] = [];
        result.push(...afterMessages.reverse());
        if (targetMessage) result.push(targetMessage);
        result.push(...beforeMessages);

        return result.map(msg => this.mapToDomain(msg));
    }

    async updateStatus(id: string, status: Message['status']): Promise<void> {
        if (!Types.ObjectId.isValid(id)) return;
        await this.db.update<MessageDB>('messages', { _id: new Types.ObjectId(id) }, { status });
    }

    async updateContent(id: string, content: string, updatedAt: Date): Promise<void> {
        if (!Types.ObjectId.isValid(id)) return;
        const message = await this.db.findOne<MessageDB>('messages', { _id: new Types.ObjectId(id) });
        if (!message) return;

        const oldContent = message.content;
        const oldUpdatedAt = message.updated_at || message.created_at;

        await this.db.update<MessageDB>('messages', { _id: new Types.ObjectId(id) }, { 
            $set: { content, updated_at: updatedAt, is_edited: true },
            $push: { edit_history: { content: oldContent, updated_at: oldUpdatedAt } }
        } as any);
    }

    async updateByFileKey(fileKey: string, updateData: Partial<MessageDB>): Promise<Message | null> {
        const result = await this.db.update<MessageDB>('messages', { "video.file_key": fileKey } as any, updateData);
        if (Array.isArray(result) && result.length > 0) {
            return this.mapToDomain(result[0]!);
        } else if (result && !Array.isArray(result)) {
            return this.mapToDomain(result);
        }
        return null;
    }

    async toggleReaction(message: Message, userId: string, emoji: string): Promise<Message> {
        const reactions = message.reactions?.map(r => ({
            user_id: r.user_id.toString(),
            emoji: r.emoji
        })) || [];

        const existingReactionIndex = reactions.findIndex(r => r.user_id === userId && r.emoji === emoji);

        let newReactions;
        if (existingReactionIndex > -1) {
            newReactions = reactions.filter((_, index) => index !== existingReactionIndex);
        } else {
            newReactions = [...reactions, { user_id: userId, emoji }];
        }

        const messageId = message.id;

        let updated = await this.db.update<MessageDB>(
            'messages',
            { _id: new Types.ObjectId(messageId) },
            { reactions: newReactions }
        );

        if (!updated) {
            throw createHttpError(500, 'Failed to update reaction');
        }
        
        const updatedDoc = Array.isArray(updated) ? updated[0] : updated;
        return this.mapToDomain(updatedDoc);
    }

    private mapToDomain(doc: any): Message {
        if (!doc) return doc;
        const { _id, __v, ...rest } = doc;
        return {
            ...rest,
            id: _id?.toString()
        } as Message;
    }
}
