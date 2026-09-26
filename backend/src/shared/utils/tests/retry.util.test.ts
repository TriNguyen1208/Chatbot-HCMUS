import { describe, it, expect, vi } from "vitest";
import { retryWithBackoff, calculateJitterDelay } from "../retry.util.js";

describe("retry.util", () => {
    describe("calculateJitterDelay", () => {
        it("should return a number within [0, min(maxDelay, baseDelay * 2^attempt)]", () => {
            const baseDelay = 100;
            const maxDelay = 1000;
            for (let attempt = 1; attempt <= 5; attempt++) {
                const delay = calculateJitterDelay(attempt, baseDelay, maxDelay);
                const limit = Math.min(maxDelay, baseDelay * Math.pow(2, attempt));
                expect(delay).toBeGreaterThanOrEqual(0);
                expect(delay).toBeLessThanOrEqual(limit);
            }
        });
    });

    describe("retryWithBackoff", () => {
        it("should return the result immediately on first success without retrying", async () => {
            const fn = vi.fn().mockResolvedValue("SUCCESS");
            const result = await retryWithBackoff(fn, { maxRetries: 3 });

            expect(result).toBe("SUCCESS");
            expect(fn).toHaveBeenCalledTimes(1);
        });

        it("should retry until successful if within maxRetries", async () => {
            const fn = vi
                .fn()
                .mockRejectedValueOnce(new Error("Network glitch 1"))
                .mockRejectedValueOnce(new Error("Network glitch 2"))
                .mockResolvedValue("RECOVERED");

            const onRetry = vi.fn();

            const result = await retryWithBackoff(fn, {
                maxRetries: 3,
                baseDelayMs: 10,
                maxDelayMs: 50,
                onRetry,
            });

            expect(result).toBe("RECOVERED");
            expect(fn).toHaveBeenCalledTimes(3);
            expect(onRetry).toHaveBeenCalledTimes(2);
        });

        it("should re-throw the error when maxRetries is exceeded", async () => {
            const fn = vi.fn().mockRejectedValue(new Error("Permanent server error"));

            await expect(
                retryWithBackoff(fn, {
                    maxRetries: 2,
                    baseDelayMs: 5,
                    maxDelayMs: 20,
                })
            ).rejects.toThrow("Permanent server error");

            expect(fn).toHaveBeenCalledTimes(3); // 1 initial + 2 retries
        });

        it("should not retry if shouldRetry returns false", async () => {
            const fn = vi.fn().mockRejectedValue(new Error("400 Bad Request"));

            await expect(
                retryWithBackoff(fn, {
                    maxRetries: 3,
                    baseDelayMs: 10,
                    shouldRetry: (err) => !err.message.includes("400"),
                })
            ).rejects.toThrow("400 Bad Request");

            expect(fn).toHaveBeenCalledTimes(1);
        });
    });
});
