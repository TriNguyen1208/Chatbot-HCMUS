import { describe, it, expect, vi, beforeEach } from "vitest";
import { globalRateLimiter, authRateLimiter, mediaRateLimiter } from "../rate-limit.middleware.js";

describe("RateLimit Middlewares", () => {
    it("should export configured rate limiters", () => {
        expect(globalRateLimiter).toBeDefined();
        expect(authRateLimiter).toBeDefined();
        expect(mediaRateLimiter).toBeDefined();
        expect(typeof globalRateLimiter).toBe("function");
        expect(typeof authRateLimiter).toBe("function");
        expect(typeof mediaRateLimiter).toBe("function");
    });
});
