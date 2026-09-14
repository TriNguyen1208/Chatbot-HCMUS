import { redisClient } from "#@/infrastructure/redis/redis.client.js";

const DEFAULT_TTL = 7 * 24 * 3600; // 7 days

export interface ConversationMetaCache {
    name?: string;
    avatar_url?: string;
    type: string;
    primary_icon?: string;
    is_active: boolean;
    block_by?: string;
    block_at?: string;
}

export class ConversationCache {
    private convKey(convId: string): string {
        return `conversation:${convId}`;
    }

    private userConvsKey(userId: string): string {
        return `user:${userId}:convs`;
    }

    private directConvKey(u1: string, u2: string): string {
        const sorted = [u1, u2].sort();
        return `conv:direct:${sorted[0]}:${sorted[1]}`;
    }

    private selfConvKey(userId: string): string {
        return `conv:self:${userId}`;
    }

    // --- Full Conversation Entity Cache (Single Source of Truth) ---
    async getConversation<T = any>(convId: string): Promise<T | null> {
        return await redisClient.getJSON<T>(this.convKey(convId));
    }

    async setConversation(convId: string, data: any, ttlSeconds: number = DEFAULT_TTL): Promise<void> {
        await redisClient.setJSON(this.convKey(convId), data, ttlSeconds);
    }

    async invalidateConversation(convId: string): Promise<void> {
        await redisClient.del(this.convKey(convId));
    }

    // --- Derived getters directly from conversation:${convId} ---
    async isMember(convId: string, userId: string): Promise<boolean> {
        const conv = await this.getConversation(convId);
        if (!conv || conv.is_active === false) return false;
        const memberIds = conv.member_ids?.map((id: any) => id.toString()) || [];
        return memberIds.includes(userId);
    }

    async getMembers(convId: string): Promise<string[]> {
        const conv = await this.getConversation(convId);
        if (!conv || conv.is_active === false) return [];
        return conv.member_ids?.map((id: any) => id.toString()) || [];
    }

    async isAdmin(convId: string, userId: string): Promise<boolean> {
        const conv = await this.getConversation(convId);
        if (!conv || conv.is_active === false) return false;
        const adminIds = conv.admin_ids?.map((id: any) => id.toString()) || [];
        return adminIds.includes(userId);
    }

    async getAdmins(convId: string): Promise<string[]> {
        const conv = await this.getConversation(convId);
        if (!conv || conv.is_active === false) return [];
        return conv.admin_ids?.map((id: any) => id.toString()) || [];
    }

    async getMeta(convId: string): Promise<ConversationMetaCache | null> {
        const conv = await this.getConversation(convId);
        if (!conv) return null;
        return {
            name: conv.name,
            avatar_url: conv.avatar_url,
            type: conv.type,
            primary_icon: conv.primary_icon,
            is_active: conv.is_active ?? true,
            block_by: conv.block?.block_by?.toString(),
            block_at: conv.block?.block_at ? new Date(conv.block.block_at).toISOString() : undefined,
        };
    }

    // --- User Conversations (Set) ---
    async getUserConvs(userId: string): Promise<string[]> {
        const key = this.userConvsKey(userId);
        return await redisClient.smembers(key);
    }

    async setUserConvs(userId: string, convIds: (string | object)[]): Promise<void> {
        const key = this.userConvsKey(userId);
        const strIds = convIds.map(id => id.toString());
        await redisClient.del(key);
        if (strIds.length > 0) {
            await redisClient.sadd(key, ...strIds);
            await redisClient.expire(key, DEFAULT_TTL);
        }
    }

    async addUserConv(userId: string, convId: string): Promise<void> {
        const key = this.userConvsKey(userId);
        await redisClient.sadd(key, convId);
        await redisClient.expire(key, DEFAULT_TTL);
    }

    async removeUserConv(userId: string, convId: string): Promise<void> {
        const key = this.userConvsKey(userId);
        await redisClient.srem(key, convId);
    }

    // --- Direct & Self Conversation ID Cache ---
    async getDirectConvId(u1: string, u2: string): Promise<string | null> {
        const key = this.directConvKey(u1, u2);
        return await redisClient.get(key);
    }

    async setDirectConvId(u1: string, u2: string, convId: string): Promise<void> {
        const key = this.directConvKey(u1, u2);
        await redisClient.set(key, convId, DEFAULT_TTL);
    }

    async getSelfConvId(userId: string): Promise<string | null> {
        const key = this.selfConvKey(userId);
        return await redisClient.get(key);
    }

    async setSelfConvId(userId: string, convId: string): Promise<void> {
        const key = this.selfConvKey(userId);
        await redisClient.set(key, convId, DEFAULT_TTL);
    }
}
