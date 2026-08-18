import type { Message } from "../entities/message.entity.js";
import type { IDatabase } from "#@/infrastructure/database/database.interface.js";
import { Types } from "mongoose";

export class MessageRepository {
    constructor(
        private readonly db: IDatabase,
        private readonly supabaseDB: IDatabase
    ) {}

    /**
     * Populates sender details for an array of messages by fetching data from the user database.
     * @param messages An array of raw message objects.
     * @returns An array of messages with populated sender objects.
     */
    private async populateSender(messages: Message[]): Promise<any[]> {
        if (!messages.length) return [];

        const senderIds = new Set<string>();
        messages.forEach(m => {
            if (m.sender_id && m.sender_id !== 'system') senderIds.add(m.sender_id);
        });

        let usersMap = new Map<string, any>();
        if (senderIds.size > 0) {
            const users = await this.supabaseDB.findIn('users', 'id', Array.from(senderIds));
            users.forEach((u: any) => usersMap.set(u.id, u));
        }

        return messages.map(m => {
            const mappedConversation = (m as any).conversation_id && typeof (m as any).conversation_id === 'object' 
                ? (m as any).conversation_id 
                : null;
            const { conversation_id, sender_id, ...rest } = m as any;
            return {
                ...rest,
                sender: m.sender_id === 'system' 
                    ? { id: 'system', name: 'System', avatar_url: '/CR_LM_Chess.jpg' } 
                    : usersMap.get(m.sender_id),
                conversation: mappedConversation
            };
        });
    }

    /**
     * Creates a new message record in the database.
     * @param messageData The message data to insert.
     * @returns The created message.
     */
    async create(messageData: Message): Promise<Message> {
        if (typeof messageData.conversation_id === 'string' && Types.ObjectId.isValid(messageData.conversation_id)) {
            messageData.conversation_id = new Types.ObjectId(messageData.conversation_id);
        }
        const result = await this.db.insert<Message>('messages', messageData);
        return (Array.isArray(result) ? result[0] : result) as Message;
    }

    /**
     * Finds a message by its ID and populates its sender.
     * @param id The message ID.
     * @returns The message object, or null if not found.
     */
    async findByID(id: string): Promise<Message | null> {
        if (!Types.ObjectId.isValid(id)) return null;
        const msg = await this.db.findOne<Message>('messages', { _id: new Types.ObjectId(id) } as any, { populate: 'conversation_id' });
        if (!msg) return null;
        const populated = await this.populateSender([msg]);
        return populated[0];
    }

    /**
     * Retrieves a paginated list of sent messages for a conversation.
     * @param conversationId The conversation ID.
     * @param limit The maximum number of results.
     * @param cursorId The cursor for pagination (message ID).
     * @returns An array of populated messages.
     */
    async getMessages(conversationId: string, limit: number = 20, cursorId?: string): Promise<Message[]> {
        const conditions: any = { conversation_id: conversationId, status: { $in: ['sent', 'recalled'] } };
        
        if (cursorId) {
            conditions._id = { $lt: new Types.ObjectId(cursorId) };
        }
        
        const messages = await this.db.query<Message>('messages', conditions, {
            limit,
            orderBy: { field: '_id' as any, ascending: false },
            populate: 'conversation_id'
        });
        return await this.populateSender(messages) as Message[];
    }

    /**
     * Updates the status of a message (e.g., to 'recalled').
     * @param id The message ID.
     * @param status The new status.
     */
    async updateStatus(id: string, status: Message['status']): Promise<void> {
        if (!Types.ObjectId.isValid(id)) return;
        await this.db.update<Message>('messages', { _id: new Types.ObjectId(id) } as any, { status });
    }

    /**
     * Updates the text content of a message and marks it as edited.
     * @param id The message ID.
     * @param content The new text content.
     * @param updatedAt The timestamp of the update.
     */
    async updateContent(id: string, content: string, updatedAt: Date): Promise<void> {
        if (!Types.ObjectId.isValid(id)) return;
        await this.db.update<Message>('messages', { _id: new Types.ObjectId(id) } as any, { 
            content, 
            updated_at: updatedAt, 
            is_edited: true 
        });
    }

    /**
     * Updates a message using its associated video file key (e.g., when background processing finishes).
     * @param fileKey The video file key.
     * @param updateData The fields to update (status, video URLs, etc.).
     * @returns The updated message, or null if not found.
     */
    async updateByFileKey(fileKey: string, updateData: Partial<Message>): Promise<Message | null> {
        const result = await this.db.update<Message>('messages', { "video.file_key": fileKey } as any, updateData);
        if (Array.isArray(result) && result.length > 0) {
            return result[0]!;
        } else if (result && !Array.isArray(result)) {
            return result;
        }
        return null;
    }
}
