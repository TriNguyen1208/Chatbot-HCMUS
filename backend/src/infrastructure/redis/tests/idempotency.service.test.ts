import { describe, it, expect, vi, beforeEach } from "vitest";
import { IdempotencyService } from "../idempotency.service.js";
import type { RedisClient } from "../redis.client.js";

describe("IdempotencyService", () => {
    let mockRedis: Partial<RedisClient>;
    let service: IdempotencyService;

    beforeEach(() => {
        mockRedis = {
            set: vi.fn(),
            getJSON: vi.fn(),
            setJSON: vi.fn(),
            del: vi.fn(),
        };
        service = new IdempotencyService(mockRedis as RedisClient);
    });

    it("should execute action on first request and cache the result", async () => {
        // Lock acquired successfully (set with NX returns true)
        (mockRedis.set as any).mockResolvedValue(true);
        (mockRedis.setJSON as any).mockResolvedValue(undefined);

        const action = vi.fn().mockResolvedValue({ id: "msg_123", content: "Hello" });

        const result = await service.execute("msg:key_1", 60, action);

        expect(result.isDuplicate).toBe(false);
        expect(result.data).toEqual({ id: "msg_123", content: "Hello" });
        expect(action).toHaveBeenCalledTimes(1);
        expect(mockRedis.set).toHaveBeenCalledWith(
            "idempotency:msg:key_1",
            JSON.stringify({ status: "PROCESSING" }),
            30,
            true
        );
        expect(mockRedis.setJSON).toHaveBeenCalledWith(
            "idempotency:msg:key_1",
            { status: "COMPLETED", data: { id: "msg_123", content: "Hello" } },
            60
        );
    });

    it("should return cached result without calling action when duplicate request occurs", async () => {
        // Lock acquisition failed (key already exists, set with NX returns false)
        (mockRedis.set as any).mockResolvedValue(false);
        (mockRedis.getJSON as any).mockResolvedValue({
            status: "COMPLETED",
            data: { id: "msg_123", content: "Cached result" },
        });

        const action = vi.fn().mockResolvedValue({ id: "msg_should_not_run" });

        const result = await service.execute("msg:key_1", 60, action);

        expect(result.isDuplicate).toBe(true);
        expect(result.data).toEqual({ id: "msg_123", content: "Cached result" });
        expect(action).not.toHaveBeenCalled();
    });

    it("should fail-fast and throw Conflict error immediately if status is PROCESSING", async () => {
        // Lock acquisition failed (key already exists, set with NX returns false)
        (mockRedis.set as any).mockResolvedValue(false);
        (mockRedis.getJSON as any).mockResolvedValue({
            status: "PROCESSING",
        });

        const action = vi.fn().mockResolvedValue({ id: "msg_should_not_run" });

        await expect(service.execute("msg:key_1", 60, action)).rejects.toThrow(
            "Yêu cầu trước đó đang được xử lý, vui lòng không thao tác liên tục."
        );
        expect(action).not.toHaveBeenCalled();
    });

    it("should delete the lock key if action throws an error so client can retry", async () => {
        (mockRedis.set as any).mockResolvedValue(true);
        (mockRedis.del as any).mockResolvedValue(undefined);

        const action = vi.fn().mockRejectedValue(new Error("Database disconnected"));

        await expect(service.execute("msg:key_fail", 60, action)).rejects.toThrow("Database disconnected");

        expect(mockRedis.del).toHaveBeenCalledWith("idempotency:msg:key_fail");
    });
});
