import type { Conversation } from "../entities/conversation.entity.js";
import type { IDatabase } from "#@/infrastructure/database/database.interface.js";
import { Types } from "mongoose";

export interface IConversationRepository {

    create(data: Conversation): Promise<Conversation>;

    findByID(id: string): Promise<Conversation | null>;

    checkUserInConversation(conversationId: string, userId: string): Promise<boolean>;

    getConversationsByUser(userId: string, limit?: number, cursorId?: string, type?: 'utu' | 'group'): Promise<Conversation[]>;

    findDirectConversation(user1: string, user2: string): Promise<Conversation | null>;

    addMembers(conversationId: string, userIds: string[]): Promise<void>;

    removeMember(conversationId: string, userId: string): Promise<void>;

    updateLastMessage(conversationId: string, messageId: string): Promise<void>;

    removeMembers(conversationId: string, userIds: string[]): Promise<void>;

    addAdmins(conversationId: string, adminIds: string[]): Promise<void>;
}

export class ConversationRepository implements IConversationRepository {
    constructor(
        private readonly db: IDatabase,
        private readonly supabaseDB: IDatabase
    ) { }

    /**
     * Populates user details (members, admins, and the last message sender) for an array of conversations.
     * Fetches user data from the user database to replace raw IDs with user objects.
     * @param conversations An array of raw conversation objects from the database.
     * @returns An array of conversations with populated member, admin, and last message objects.
     */
    private async populateUsers(conversations: Conversation[]): Promise<any[]> {
        if (!conversations.length) return [];

        const userIds = new Set<string>();
        conversations.forEach(c => {
            c.member_ids?.forEach(id => { if (id !== 'system') userIds.add(id); });
            c.admin_ids?.forEach(id => { if (id !== 'system') userIds.add(id); });
            if ((c as any).last_message_id && (c as any).last_message_id.sender_id) {
                const senderId = (c as any).last_message_id.sender_id;
                if (senderId !== 'system') userIds.add(senderId);
            }
        });

        let usersMap = new Map<string, any>();
        if (userIds.size > 0) {
            const users = await this.supabaseDB.findIn('users', 'id', Array.from(userIds));
            users.forEach((u: any) => usersMap.set(u.id, u));
        }

        return conversations.map(c => {
            const mappedMembers = c.member_ids?.map(id => usersMap.get(id)).filter(Boolean) || [];
            const mappedAdmins = c.admin_ids?.map(id => usersMap.get(id)).filter(Boolean) || [];

            let mappedLastMessage: any = (c as any).last_message_id && typeof (c as any).last_message_id === 'object'
                ? (c as any).last_message_id
                : null;

            if (mappedLastMessage && mappedLastMessage.sender_id) {
                mappedLastMessage = {
                    ...mappedLastMessage,
                    sender: mappedLastMessage.sender_id === 'system'
                        ? { id: 'system', name: 'System', avatar_url: '/CR_LM_Chess.jpg' }
                        : usersMap.get(mappedLastMessage.sender_id)
                };
            }

            const { last_message_id, member_ids, admin_ids, ...rest } = c as any;

            return {
                ...rest,
                members: mappedMembers,
                admins: mappedAdmins,
                last_message: mappedLastMessage
            };
        });
    }
    /**
     * Creates a new conversation in the database.
     * @param data The conversation data to insert.
     * @returns The created conversation, populated with member details.
     */
    async create(data: Conversation): Promise<Conversation> {
        const result = await this.db.insert<Conversation>('conversations', data);
        if (!result) throw new Error("Creating conversation failed");

        const resultArray = Array.isArray(result) ? result : [result];
        const populated = await this.populateUsers(resultArray);

        return populated[0] as Conversation;
    }
    /**
     * Finds a conversation by its ID.
     * @param id The conversation ID.
     * @returns The conversation, or null if not found.
     */
    async findByID(id: string): Promise<Conversation | null> {
        if (!Types.ObjectId.isValid(id)) return null;
        const conversation = await this.db.findOne<Conversation>('conversations', { _id: new Types.ObjectId(id) } as any, { populate: 'last_message_id' });
        if (!conversation) return null;

        const populated = await this.populateUsers([conversation]);
        return populated[0];
    }
    /**
     * Checks if a user is a member of a specific conversation.
     * @param conversationId The conversation ID.
     * @param userId The user ID.
     * @returns True if the user is a member, false otherwise.
     */
    async checkUserInConversation(conversationId: string, userId: string): Promise<boolean> {
        const conversation = await this.findByID(conversationId);
        if (!conversation) return false;

        // Duyệt qua mảng members (đã populate) để kiểm tra
        return (conversation as any).members?.some((member: any) => member.id === userId) || false;
    }
    /**
     * Retrieves a paginated list of conversations for a user.
     * @param userId The user ID.
     * @param limit The maximum number of results.
     * @param cursorId The cursor for pagination.
     * @param type The type of conversation to filter by.
     * @returns An array of conversations.
     */
    async getConversationsByUser(userId: string, limit: number = 20, cursorId?: string, type?: 'utu' | 'group'): Promise<Conversation[]> {
        const conditions: any = { member_ids: userId, last_message_id: { $ne: null } };

        if (type) {
            conditions.type = type;
        }

        if (cursorId) {
            conditions.last_message_id = { $lt: cursorId };
        }

        const conversations = await this.db.query<Conversation>('conversations', conditions, {
            limit,
            orderBy: { field: 'last_message_id', ascending: false },
            populate: 'last_message_id'
        });

        return await this.populateUsers(conversations) as Conversation[];
    }
    /**
     * Finds a direct (1-on-1) conversation between two users.
     * @param user1 The first user's ID.
     * @param user2 The second user's ID.
     * @returns The conversation, or null if it doesn't exist.
     */
    async findDirectConversation(user1: string, user2: string): Promise<Conversation | null> {
        const conditions: any = {
            type: 'utu',
            member_ids: { $all: [user1, user2], $size: 2 }
        };
        return await this.db.findOne<Conversation>('conversations', conditions);
    }
    /**
     * Adds multiple members to a conversation.
     * @param conversationId The conversation ID.
     * @param userIds The array of user IDs to add.
     */
    async addMembers(conversationId: string, userIds: string[]): Promise<void> {
        if (!Types.ObjectId.isValid(conversationId)) return;
        const conv = await this.findByID(conversationId) as any;
        if (conv) {
            const currentMemberIds = conv.members?.map((m: any) => m.id || m._id?.toString()) || [];
            const newMembers = Array.from(new Set([...currentMemberIds, ...userIds]));
            if (newMembers.length > currentMemberIds.length) {
                await this.db.update('conversations', { _id: new Types.ObjectId(conversationId) } as any, { member_ids: newMembers });
            }
        }
    }
    /**
     * Removes a single member from a conversation.
     * @param conversationId The conversation ID.
     * @param userId The ID of the user to remove.
     */
    async removeMember(conversationId: string, userId: string): Promise<void> {
        if (!Types.ObjectId.isValid(conversationId)) return;
        const conv = await this.findByID(conversationId) as any;
        if (conv) {
            const currentMemberIds = conv.members?.map((m: any) => m.id || m._id?.toString()) || [];
            if (currentMemberIds.includes(userId)) {
                const newMembers = currentMemberIds.filter((id: string) => id !== userId);
                const currentAdminIds = conv.admins?.map((a: any) => a.id || a._id?.toString()) || [];
                const newAdmins = currentAdminIds.filter((id: string) => id !== userId);
                await this.db.update('conversations', { _id: new Types.ObjectId(conversationId) } as any, { member_ids: newMembers, admin_ids: newAdmins });
            }
        }
    }
    /**
     * Removes multiple members from a conversation.
     * @param conversationId The conversation ID.
     * @param userIds The array of user IDs to remove.
     */
    async removeMembers(conversationId: string, userIds: string[]): Promise<void> {
        if (!Types.ObjectId.isValid(conversationId)) return;
        const conv = await this.findByID(conversationId) as any;
        if (conv) {
            const currentMemberIds = conv.members?.map((m: any) => m.id || m._id?.toString()) || [];
            const currentAdminIds = conv.admins?.map((a: any) => a.id || a._id?.toString()) || [];
            
            const removeSet = new Set(userIds);
            const newMembers = currentMemberIds.filter((id: string) => !removeSet.has(id));
            const newAdmins = currentAdminIds.filter((id: string) => !removeSet.has(id));
            await this.db.update('conversations', { _id: new Types.ObjectId(conversationId) } as any, { member_ids: newMembers, admin_ids: newAdmins });
        }
    }
    /**
     * Adds admins to a conversation.
     * @param conversationId The conversation ID.
     * @param adminIds The array of admin IDs to add.
     */
    async addAdmins(conversationId: string, adminIds: string[]): Promise<void> {
        if (!Types.ObjectId.isValid(conversationId)) return;
        const conv = await this.findByID(conversationId) as any;
        if (conv) {
            const currentAdminIds = conv.admins?.map((a: any) => a.id || a._id?.toString()) || [];
            const newAdmins = Array.from(new Set([...currentAdminIds, ...adminIds]));
            await this.db.update('conversations', { _id: new Types.ObjectId(conversationId) } as any, { admin_ids: newAdmins });
        }
    }
    /**
     * Updates the last message ID of a conversation.
     * @param conversationId The conversation ID.
     * @param messageId The new last message ID.
     */
    async updateLastMessage(conversationId: string, messageId: string): Promise<void> {
        if (!Types.ObjectId.isValid(conversationId) || !Types.ObjectId.isValid(messageId)) return;
        await this.db.update('conversations', { _id: new Types.ObjectId(conversationId) } as any, { last_message_id: new Types.ObjectId(messageId) });
    }
}
