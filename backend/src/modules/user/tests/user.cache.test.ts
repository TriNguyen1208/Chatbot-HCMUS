import { describe, it, expect, vi, beforeEach } from "vitest";
import { UserCache } from "../user.cache.js";
import { redisClient } from "#@/infrastructure/redis/redis.client.js";

vi.mock("#@/infrastructure/redis/redis.client.js", () => ({
    redisClient: {
        getJSON: vi.fn(),
        setJSON: vi.fn(),
        set: vi.fn(),
        del: vi.fn(),
        mget: vi.fn(),
        delByPattern: vi.fn()
    }
}));

describe("UserCache", () => {
    let cache: UserCache;

    beforeEach(() => {
        vi.clearAllMocks();
        cache = new UserCache();
    });

    it("should get user by id", async () => {
        vi.mocked(redisClient.getJSON).mockResolvedValue({ id: "u1", name: "User 1" });
        const user = await cache.getUser("u1");
        expect(user).toEqual({ id: "u1", name: "User 1" });
        expect(redisClient.getJSON).toHaveBeenCalledWith("user:u1");
    });

    it("should set user with TTL", async () => {
        const testUser = { id: "u1", name: "User 1", email: "u1@test.com" };
        await cache.setUser("u1", testUser as any, 1800);
        expect(redisClient.setJSON).toHaveBeenCalledWith("user:u1", testUser, 1800);
    });

    it("should invalidate user", async () => {
        await cache.invalidateUser("u1");
        expect(redisClient.del).toHaveBeenCalledWith("user:u1");
    });

    it("should get bulk users", async () => {
        vi.mocked(redisClient.getJSON)
            .mockResolvedValueOnce({ id: "u1", name: "User 1" })
            .mockResolvedValueOnce(null);

        const users = await cache.getBulkUsers(["u1", "u2"]);
        expect(users).toHaveLength(2);
        expect(users[0]).toEqual({ id: "u1", name: "User 1" });
        expect(users[1]).toBeNull();
    });

    it("should set bulk users", async () => {
        const users = [
            { id: "u1", name: "User 1" },
            { id: "u2", name: "User 2" }
        ];
        await cache.setBulkUsers(users as any);
        expect(redisClient.setJSON).toHaveBeenCalledWith("user:u1", users[0], 3600);
        expect(redisClient.setJSON).toHaveBeenCalledWith("user:u2", users[1], 3600);
    });

    it("should get presences for user IDs", async () => {
        vi.mocked(redisClient.mget).mockResolvedValue(["online", "offline"]);
        const presences = await cache.getPresences(["u1", "u2"]);
        expect(presences).toEqual(["online", "offline"]);
        expect(redisClient.mget).toHaveBeenCalledWith(["presence:u1", "presence:u2"]);
    });

    it("should set presence online", async () => {
        await cache.setPresenceOnline("u1");
        expect(redisClient.set).toHaveBeenCalledWith("presence:u1", "online", 24 * 3600);
    });

    it("should set presence offline", async () => {
        const lastActive = new Date();
        await cache.setPresenceOffline("u1", lastActive);
        expect(redisClient.setJSON).toHaveBeenCalledWith("presence:u1", {
            status: "offline",
            last_active: lastActive
        }, 24 * 3600);
    });
});
