import { redisClient } from "#@/infrastructure/redis/redis.client.js";
import type { User } from "./user.entity.js";

const DEFAULT_TTL = 3600; // 1 hour

export class UserCache {
    private userKey(userId: string): string {
        return `user:${userId}`;
    }

    private presenceKey(userId: string): string {
        return `presence:${userId}`;
    }

    async getUser(userId: string): Promise<User | null> {
        return await redisClient.getJSON<User>(this.userKey(userId));
    }

    async setUser(userId: string, user: User, ttl: number = DEFAULT_TTL): Promise<void> {
        await redisClient.setJSON(this.userKey(userId), user, ttl);
    }

    async invalidateUser(userId: string): Promise<void> {
        await redisClient.del(this.userKey(userId));
    }

    async getBulkUsers(userIds: string[]): Promise<(User | null)[]> {
        if (userIds.length === 0) return [];
        return await Promise.all(userIds.map(id => this.getUser(id)));
    }

    async setBulkUsers(users: User[], ttl: number = DEFAULT_TTL): Promise<void> {
        if (users.length === 0) return;
        await Promise.all(users.map(u => {
            const uid = u.id?.toString();
            return uid ? this.setUser(uid, u, ttl) : Promise.resolve();
        }));
    }

    async getPresences(userIds: string[]): Promise<(string | null)[]> {
        if (userIds.length === 0) return [];
        const keys = userIds.map(id => this.presenceKey(id));
        return await redisClient.mget(keys);
    }

    async setPresenceOnline(userId: string, ttl: number = 24 * 3600): Promise<void> {
        await redisClient.set(this.presenceKey(userId), "online", ttl);
    }

    async setPresenceOffline(userId: string, lastActive: Date, ttl: number = 24 * 3600): Promise<void> {
        await redisClient.setJSON(this.presenceKey(userId), {
            status: 'offline',
            last_active: lastActive
        }, ttl);
    }
}
