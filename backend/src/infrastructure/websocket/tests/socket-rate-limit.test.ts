import { describe, it, expect, vi, beforeEach } from "vitest";
import { checkSocketRateLimit } from "../socket-rate-limit.js";
import { redisClient } from "#@/infrastructure/redis/redis.client.js";

describe("SocketRateLimiter", () => {
    let mockClient: {
        incr: ReturnType<typeof vi.fn>;
        expire: ReturnType<typeof vi.fn>;
        ttl: ReturnType<typeof vi.fn>;
    };

    beforeEach(() => {
        vi.clearAllMocks();
        mockClient = {
            incr: vi.fn(),
            expire: vi.fn(),
            ttl: vi.fn(),
        };
        vi.spyOn(redisClient, "getClient").mockReturnValue(mockClient as any);
    });

    it("should allow request on first hit and set TTL", async () => {
        mockClient.incr.mockResolvedValue(1);
        mockClient.expire.mockResolvedValue(1);
        mockClient.ttl.mockResolvedValue(60);

        const result = await checkSocketRateLimit({
            key: "test:user1",
            limit: 5,
            windowSeconds: 60,
        });

        expect(result.allowed).toBe(true);
        expect(result.remaining).toBe(4);
        expect(result.retryAfterSeconds).toBe(0);
        expect(mockClient.incr).toHaveBeenCalledWith("rl:ws:test:user1");
        expect(mockClient.expire).toHaveBeenCalledWith("rl:ws:test:user1", 60);
    });

    it("should allow request within limit without resetting TTL", async () => {
        mockClient.incr.mockResolvedValue(4);
        mockClient.ttl.mockResolvedValue(45);

        const result = await checkSocketRateLimit({
            key: "test:user1",
            limit: 5,
            windowSeconds: 60,
        });

        expect(result.allowed).toBe(true);
        expect(result.remaining).toBe(1);
        expect(result.retryAfterSeconds).toBe(0);
        expect(mockClient.expire).not.toHaveBeenCalled();
    });

    it("should block request and return retryAfterSeconds when exceeding limit", async () => {
        mockClient.incr.mockResolvedValue(6);
        mockClient.ttl.mockResolvedValue(30);

        const result = await checkSocketRateLimit({
            key: "test:user1",
            limit: 5,
            windowSeconds: 60,
        });

        expect(result.allowed).toBe(false);
        expect(result.remaining).toBe(0);
        expect(result.retryAfterSeconds).toBe(30);
    });

    it("should fail-open when Redis throws error", async () => {
        mockClient.incr.mockRejectedValue(new Error("Redis disconnected"));

        const result = await checkSocketRateLimit({
            key: "test:user1",
            limit: 5,
            windowSeconds: 60,
        });

        expect(result.allowed).toBe(true);
        expect(result.remaining).toBe(1);
        expect(result.retryAfterSeconds).toBe(0);
    });
});
