import createHttpError from "http-errors";
import { redisClient, RedisClient } from "./redis.client.js";

export interface IdempotencyResult<T> {
    isDuplicate: boolean;
    data: T;
}

interface IdempotencyRecord<T> {
    status: "PROCESSING" | "COMPLETED";
    data?: T;
}

export class IdempotencyService {
    private readonly prefix = "idempotency:";

    constructor(private readonly redis: RedisClient) { }

    /**
     * Executes an operation with Idempotency protection backed by Redis.
     * Prevents duplicate execution across user retries, network glitches, or concurrent double-clicks.
     * 
     * @param key Unique idempotency identifier (e.g. "msg:uuid-123" or "group:uuid-456")
     * @param ttlSeconds How long the idempotency result remains cached
     * @param action The actual business operation to execute if this is the first attempt
     */
    async execute<T>(
        key: string,
        ttlSeconds: number,
        action: () => Promise<T>
    ): Promise<IdempotencyResult<T>> {
        const fullKey = `${this.prefix}${key}`;

        // 1. Try to acquire atomic distributed lock (NX = true)
        const lockAcquired = await this.redis.set(
            fullKey,
            JSON.stringify({ status: "PROCESSING" }),
            Math.min(ttlSeconds, 30),
            true
        );

        // 2. If lock was NOT acquired, this is a duplicate request
        if (!lockAcquired) {
            console.log(`[IdempotencyService] ⚠️ Duplicate request detected for key: ${key}`);

            const cached = await this.redis.getJSON<IdempotencyRecord<T>>(fullKey);
            if (cached && cached.status === "COMPLETED" && cached.data !== undefined) {
                return {
                    isDuplicate: true,
                    data: cached.data
                };
            }

            // Request trước đó đang xử lý (PROCESSING) -> Fail-Fast ngay lập tức
            throw createHttpError.Conflict("Yêu cầu trước đó đang được xử lý, vui lòng không thao tác liên tục.");
        }

        // 3. First request: Execute business action
        try {
            const result = await action();

            // Cache completed result with full TTL
            await this.redis.setJSON(
                fullKey,
                { status: "COMPLETED", data: result } as IdempotencyRecord<T>,
                ttlSeconds
            );

            return {
                isDuplicate: false,
                data: result
            };
        } catch (error) {
            // Evict lock immediately on business failure so subsequent retries are unblocked
            await this.redis.del(fullKey);
            throw error;
        }
    }
}

export const idempotencyService = new IdempotencyService(redisClient);
