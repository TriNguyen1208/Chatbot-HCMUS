import { redisClient } from "#@/infrastructure/redis/redis.client.js";

export interface SocketRateLimitOptions {
    /** Key định danh đối tượng (Ví dụ: `conv:create:${userId}`, `msg:send:${userId}`) */
    key: string;
    /** Số lượng request tối đa trong cửa sổ thời gian */
    limit: number;
    /** Thời gian chu kỳ tính bằng giây */
    windowSeconds: number;
}

export interface SocketRateLimitResult {
    /** true nếu được phép đi tiếp, false nếu bị chặn */
    allowed: boolean;
    /** Số lượt còn lại trong chu kỳ */
    remaining: number;
    /** Số giây cần chờ trước khi được gửi tiếp */
    retryAfterSeconds: number;
}

/**
 * Kiểm tra Rate Limit cho các sự kiện Socket.IO sử dụng Redis Atomic Counter
 */
export async function checkSocketRateLimit(
    options: SocketRateLimitOptions
): Promise<SocketRateLimitResult> {
    const { key, limit, windowSeconds } = options;
    const client = redisClient.getClient();
    const redisKey = `rl:ws:${key}`;

    try {
        // Tăng bộ đếm nguyên tử
        const current = await client.incr(redisKey);

        // Nếu là request đầu tiên trong window -> thiết lập TTL
        if (current === 1) {
            await client.expire(redisKey, windowSeconds);
        }

        // Lấy thời gian sống còn lại (TTL) để phản hồi cho client
        let ttl = await client.ttl(redisKey);
        if (ttl < 0) {
            // Đề phòng trường hợp hiếm hoi key bị thiếu TTL
            await client.expire(redisKey, windowSeconds);
            ttl = windowSeconds;
        }

        const allowed = current <= limit;
        const remaining = Math.max(0, limit - current);

        return {
            allowed,
            remaining,
            retryAfterSeconds: allowed ? 0 : ttl,
        };
    } catch (error) {
        console.error(`[SocketRateLimit] Lỗi Redis trên key ${redisKey}:`, error);
        // Fail-Open: Nếu Redis gián đoạn, vẫn cho phép người dùng thực hiện để đảm bảo trải nghiệm
        return {
            allowed: true,
            remaining: 1,
            retryAfterSeconds: 0,
        };
    }
}
