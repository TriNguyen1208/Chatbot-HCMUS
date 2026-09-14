import { redisClient } from "#@/infrastructure/redis/redis.client.js";

const RECENT_MESSAGES_LIMIT = 50;
const RECENT_TTL = 3 * 24 * 3600; // 3 days

export class MessageCache {
    private recentKey(convId: string): string {
        return `conv:${convId}:recent`;
    }

    /**
     * Đẩy tin nhắn mới nhất vào đầu danh sách (Index 0 = Tin mới nhất).
     * Luôn giữ tối đa 50 tin nhắn gần nhất.
     */
    async pushRecent(convId: string, message: any): Promise<void> {
        const key = this.recentKey(convId);
        // Chỉ lưu tin nhắn nếu cache này đang tồn tại để tránh tạo list chắp vá
        const exists = await redisClient.exists(key);
        if (!exists) return;

        const serialized = JSON.stringify(message);
        await redisClient.lpush(key, serialized);
        await redisClient.ltrim(key, 0, RECENT_MESSAGES_LIMIT - 1);
        await redisClient.expire(key, RECENT_TTL);
    }

    /**
     * Lấy các tin nhắn gần nhất (mặc định lấy 20 tin).
     * Trả về mảng rỗng nếu Cache Miss.
     */
    async getRecent(convId: string, limit: number = 20): Promise<any[] | null> {
        const key = this.recentKey(convId);
        const exists = await redisClient.exists(key);
        if (!exists) return null; // Cache miss

        const rawList = await redisClient.lrange(key, 0, Math.min(limit, RECENT_MESSAGES_LIMIT) - 1);
        return rawList.map(item => {
            try {
                return JSON.parse(item);
            } catch {
                return null;
            }
        }).filter(Boolean);
    }

    /**
     * Nạp danh sách tin nhắn từ Database vào Cache (thường là 50 tin mới nhất).
     * Giả định mảng messages truyền vào được sắp xếp từ Mới nhất -> Cũ hơn.
     */
    async setRecent(convId: string, messages: any[]): Promise<void> {
        const key = this.recentKey(convId);
        await redisClient.del(key);
        if (messages.length === 0) return;

        const slice = messages.slice(0, RECENT_MESSAGES_LIMIT);
        // Để index 0 là tin mới nhất, ta lpush theo thứ tự từ cũ nhất đến mới nhất
        const reversed = [...slice].reverse();
        const serialized = reversed.map(m => JSON.stringify(m));

        await redisClient.lpush(key, ...serialized);
        await redisClient.expire(key, RECENT_TTL);
    }

    /**
     * Cập nhật một tin nhắn trong danh sách gần đây (khi sửa tin hoặc thu hồi tin).
     */
    async updateRecent(convId: string, messageId: string, updateFn: (msg: any) => any): Promise<void> {
        const key = this.recentKey(convId);
        const exists = await redisClient.exists(key);
        if (!exists) return;

        const rawList = await redisClient.lrange(key, 0, RECENT_MESSAGES_LIMIT - 1);
        if (rawList.length === 0) return;

        let changed = false;
        const updatedList = rawList.map(item => {
            try {
                const parsed = JSON.parse(item);
                const id = parsed.id || parsed._id?.toString();
                if (id === messageId) {
                    changed = true;
                    return JSON.stringify(updateFn(parsed));
                }
                return item;
            } catch {
                return item;
            }
        });

        if (changed) {
            await redisClient.del(key);
            const reversed = [...updatedList].reverse();
            await redisClient.lpush(key, ...reversed);
            await redisClient.expire(key, RECENT_TTL);
        }
    }

    async clearRecent(convId: string): Promise<void> {
        const key = this.recentKey(convId);
        await redisClient.del(key);
    }

    // --- Watermark Cache ---
    private watermarkKey(convId: string): string {
        return `watermarks:${convId}`;
    }

    async getWatermark(convId: string, userId: string): Promise<Record<string, any>> {
        const raw = await redisClient.hget(this.watermarkKey(convId), userId);
        if (!raw) return {};
        try {
            return JSON.parse(raw);
        } catch {
            return {};
        }
    }

    async setWatermark(convId: string, userId: string, data: Record<string, any>): Promise<void> {
        await redisClient.hset(this.watermarkKey(convId), { [userId]: data });
    }

    async updateWatermark(convId: string, userId: string, messageId: string, type: 'delivered' | 'read'): Promise<Record<string, any>> {
        const current = await this.getWatermark(convId, userId);
        if (type === 'delivered') {
            current.last_delivered_msg_id = messageId;
        } else {
            current.last_read_msg_id = messageId;
        }
        await this.setWatermark(convId, userId, current);
        return current;
    }
}