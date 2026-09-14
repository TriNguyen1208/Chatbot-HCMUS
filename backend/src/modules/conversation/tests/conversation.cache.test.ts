import { describe, it, expect, vi, beforeEach } from "vitest";
import { ConversationCache } from "../conversation.cache.js";
import { redisClient } from "#@/infrastructure/redis/redis.client.js";

vi.mock("#@/infrastructure/redis/redis.client.js", () => ({
    redisClient: {
        exists: vi.fn(),
        smembers: vi.fn(),
        sadd: vi.fn(),
        srem: vi.fn(),
        del: vi.fn(),
        expire: vi.fn(),
        get: vi.fn(),
        set: vi.fn(),
        getJSON: vi.fn(),
        setJSON: vi.fn()
    }
}));

describe("ConversationCache", () => {
    let cache: ConversationCache;

    beforeEach(() => {
        vi.clearAllMocks();
        cache = new ConversationCache();
    });

    describe("Full Conversation Entity Cache (Single Source of Truth)", () => {
        it("should get conversation JSON", async () => {
            vi.mocked(redisClient.getJSON).mockResolvedValue({ id: "c1", name: "Room 1" });
            const conv = await cache.getConversation("c1");
            expect(conv).toEqual({ id: "c1", name: "Room 1" });
            expect(redisClient.getJSON).toHaveBeenCalledWith("conversation:c1");
        });

        it("should set conversation JSON with TTL", async () => {
            await cache.setConversation("c1", { id: "c1" }, 1800);
            expect(redisClient.setJSON).toHaveBeenCalledWith("conversation:c1", { id: "c1" }, 1800);
        });

        it("should invalidate conversation cache", async () => {
            await cache.invalidateConversation("c1");
            expect(redisClient.del).toHaveBeenCalledWith("conversation:c1");
        });
    });

    describe("Derived getters from conversation cache", () => {
        it("isMember should return false if conversation is not cached or inactive", async () => {
            vi.mocked(redisClient.getJSON).mockResolvedValue(null);
            expect(await cache.isMember("c1", "u1")).toBe(false);

            vi.mocked(redisClient.getJSON).mockResolvedValue({ id: "c1", is_active: false, member_ids: ["u1"] });
            expect(await cache.isMember("c1", "u1")).toBe(false);
        });

        it("isMember should return true when user is in member_ids", async () => {
            vi.mocked(redisClient.getJSON).mockResolvedValue({
                id: "c1",
                is_active: true,
                member_ids: ["u1", "u2"]
            });
            expect(await cache.isMember("c1", "u1")).toBe(true);
            expect(await cache.isMember("c1", "u3")).toBe(false);
        });

        it("getMembers should return member array", async () => {
            vi.mocked(redisClient.getJSON).mockResolvedValue({
                id: "c1",
                member_ids: ["u1", "u2"]
            });
            expect(await cache.getMembers("c1")).toEqual(["u1", "u2"]);
        });

        it("isAdmin should return true if user is in admin_ids", async () => {
            vi.mocked(redisClient.getJSON).mockResolvedValue({
                id: "c1",
                admin_ids: ["u1"]
            });
            expect(await cache.isAdmin("c1", "u1")).toBe(true);
            expect(await cache.isAdmin("c1", "u2")).toBe(false);
        });

        it("getAdmins should return admin array", async () => {
            vi.mocked(redisClient.getJSON).mockResolvedValue({
                id: "c1",
                admin_ids: ["u1"]
            });
            expect(await cache.getAdmins("c1")).toEqual(["u1"]);
        });

        it("getMeta should parse and return meta object", async () => {
            vi.mocked(redisClient.getJSON).mockResolvedValue({
                name: "Test Group",
                avatar_url: "https://example.com/avatar.png",
                type: "group",
                primary_icon: "icon-1",
                is_active: true,
                block: {
                    block_by: "u1",
                    block_at: "2026-09-13T10:00:00.000Z"
                }
            });

            const meta = await cache.getMeta("c1");
            expect(meta).toEqual({
                name: "Test Group",
                avatar_url: "https://example.com/avatar.png",
                type: "group",
                primary_icon: "icon-1",
                is_active: true,
                block_by: "u1",
                block_at: "2026-09-13T10:00:00.000Z"
            });
        });

        it("getMeta should return null if conversation not cached", async () => {
            vi.mocked(redisClient.getJSON).mockResolvedValue(null);
            expect(await cache.getMeta("c1")).toBeNull();
        });
    });

    describe("User Conversations (Set)", () => {
        it("should get user convs", async () => {
            vi.mocked(redisClient.smembers).mockResolvedValue(["c1", "c2"]);
            const convs = await cache.getUserConvs("u1");
            expect(convs).toEqual(["c1", "c2"]);
            expect(redisClient.smembers).toHaveBeenCalledWith("user:u1:convs");
        });

        it("should set user convs", async () => {
            await cache.setUserConvs("u1", ["c1", "c2"]);
            expect(redisClient.del).toHaveBeenCalledWith("user:u1:convs");
            expect(redisClient.sadd).toHaveBeenCalledWith("user:u1:convs", "c1", "c2");
            expect(redisClient.expire).toHaveBeenCalledWith("user:u1:convs", 7 * 24 * 3600);
        });

        it("should add user conv", async () => {
            await cache.addUserConv("u1", "c3");
            expect(redisClient.sadd).toHaveBeenCalledWith("user:u1:convs", "c3");
        });

        it("should remove user conv", async () => {
            await cache.removeUserConv("u1", "c3");
            expect(redisClient.srem).toHaveBeenCalledWith("user:u1:convs", "c3");
        });
    });

    describe("Direct & Self Conversation ID Cache", () => {
        it("should get direct conv id using sorted user ids", async () => {
            vi.mocked(redisClient.get).mockResolvedValue("c-direct-123");
            const convId = await cache.getDirectConvId("userB", "userA");
            expect(convId).toBe("c-direct-123");
            expect(redisClient.get).toHaveBeenCalledWith("conv:direct:userA:userB");
        });

        it("should set direct conv id using sorted user ids", async () => {
            await cache.setDirectConvId("userZ", "userM", "c-direct-999");
            expect(redisClient.set).toHaveBeenCalledWith("conv:direct:userM:userZ", "c-direct-999", 7 * 24 * 3600);
        });

        it("should get and set self conv id", async () => {
            vi.mocked(redisClient.get).mockResolvedValue("c-self-1");
            const res = await cache.getSelfConvId("u1");
            expect(res).toBe("c-self-1");
            expect(redisClient.get).toHaveBeenCalledWith("conv:self:u1");

            await cache.setSelfConvId("u1", "c-self-1");
            expect(redisClient.set).toHaveBeenCalledWith("conv:self:u1", "c-self-1", 7 * 24 * 3600);
        });
    });
});
