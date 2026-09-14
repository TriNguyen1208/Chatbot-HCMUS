import { describe, it, expect, vi, beforeEach } from "vitest";
import { MessageCache } from "../../message.cache.js";
import { redisClient } from "#@/infrastructure/redis/redis.client.js";

vi.mock("#@/infrastructure/redis/redis.client.js", () => ({
    redisClient: {
        exists: vi.fn(),
        lpush: vi.fn(),
        lrange: vi.fn(),
        ltrim: vi.fn(),
        del: vi.fn(),
        expire: vi.fn(),
        hget: vi.fn(),
        hset: vi.fn()
    }
}));

describe("MessageCache", () => {
    let cache: MessageCache;

    beforeEach(() => {
        vi.clearAllMocks();
        cache = new MessageCache();
    });

    describe("pushRecent", () => {
        it("should not push if cache does not exist", async () => {
            vi.mocked(redisClient.exists).mockResolvedValue(false);
            await cache.pushRecent("c1", { id: "m1", content: "hello" });
            expect(redisClient.lpush).not.toHaveBeenCalled();
        });

        it("should lpush and ltrim if cache exists", async () => {
            vi.mocked(redisClient.exists).mockResolvedValue(true);
            await cache.pushRecent("c1", { id: "m1", content: "hello" });
            expect(redisClient.lpush).toHaveBeenCalledWith("conv:c1:recent", JSON.stringify({ id: "m1", content: "hello" }));
            expect(redisClient.ltrim).toHaveBeenCalledWith("conv:c1:recent", 0, 49);
            expect(redisClient.expire).toHaveBeenCalledWith("conv:c1:recent", 3 * 24 * 3600);
        });
    });

    describe("getRecent", () => {
        it("should return null on cache miss", async () => {
            vi.mocked(redisClient.exists).mockResolvedValue(false);
            const res = await cache.getRecent("c1");
            expect(res).toBeNull();
        });

        it("should parse and return messages on cache hit", async () => {
            vi.mocked(redisClient.exists).mockResolvedValue(true);
            vi.mocked(redisClient.lrange).mockResolvedValue([
                JSON.stringify({ id: "m2", content: "new" }),
                JSON.stringify({ id: "m1", content: "old" })
            ]);

            const res = await cache.getRecent("c1", 20);
            expect(res).toEqual([
                { id: "m2", content: "new" },
                { id: "m1", content: "old" }
            ]);
            expect(redisClient.lrange).toHaveBeenCalledWith("conv:c1:recent", 0, 19);
        });
    });

    describe("setRecent", () => {
        it("should set recent messages in reverse order so newest is at index 0", async () => {
            const messages = [
                { id: "m2", content: "newest" },
                { id: "m1", content: "older" }
            ];

            await cache.setRecent("c1", messages);
            expect(redisClient.del).toHaveBeenCalledWith("conv:c1:recent");
            expect(redisClient.lpush).toHaveBeenCalledWith(
                "conv:c1:recent",
                JSON.stringify({ id: "m1", content: "older" }),
                JSON.stringify({ id: "m2", content: "newest" })
            );
            expect(redisClient.expire).toHaveBeenCalledWith("conv:c1:recent", 3 * 24 * 3600);
        });
    });

    describe("updateRecent", () => {
        it("should update an existing message in the list", async () => {
            vi.mocked(redisClient.exists).mockResolvedValue(true);
            vi.mocked(redisClient.lrange).mockResolvedValue([
                JSON.stringify({ id: "m2", content: "new" }),
                JSON.stringify({ id: "m1", content: "old" })
            ]);

            await cache.updateRecent("c1", "m1", (msg) => ({ ...msg, content: "updated old" }));

            expect(redisClient.del).toHaveBeenCalledWith("conv:c1:recent");
            expect(redisClient.lpush).toHaveBeenCalledWith(
                "conv:c1:recent",
                JSON.stringify({ id: "m1", content: "updated old" }),
                JSON.stringify({ id: "m2", content: "new" })
            );
        });
    });

    describe("watermarks", () => {
        it("should get empty watermark if not present", async () => {
            vi.mocked(redisClient.hget).mockResolvedValue(null);
            const res = await cache.getWatermark("c1", "u1");
            expect(res).toEqual({});
            expect(redisClient.hget).toHaveBeenCalledWith("watermarks:c1", "u1");
        });

        it("should parse watermark JSON if present", async () => {
            vi.mocked(redisClient.hget).mockResolvedValue(JSON.stringify({ last_read_msg_id: "m1" }));
            const res = await cache.getWatermark("c1", "u1");
            expect(res).toEqual({ last_read_msg_id: "m1" });
        });

        it("should update delivered watermark", async () => {
            vi.mocked(redisClient.hget).mockResolvedValue(JSON.stringify({ last_read_msg_id: "m1" }));
            const updated = await cache.updateWatermark("c1", "u1", "m2", "delivered");
            expect(updated).toEqual({ last_read_msg_id: "m1", last_delivered_msg_id: "m2" });
            expect(redisClient.hset).toHaveBeenCalledWith("watermarks:c1", {
                u1: { last_read_msg_id: "m1", last_delivered_msg_id: "m2" }
            });
        });

        it("should update read watermark", async () => {
            vi.mocked(redisClient.hget).mockResolvedValue(null);
            const updated = await cache.updateWatermark("c1", "u1", "m3", "read");
            expect(updated).toEqual({ last_read_msg_id: "m3" });
            expect(redisClient.hset).toHaveBeenCalledWith("watermarks:c1", {
                u1: { last_read_msg_id: "m3" }
            });
        });
    });
});
