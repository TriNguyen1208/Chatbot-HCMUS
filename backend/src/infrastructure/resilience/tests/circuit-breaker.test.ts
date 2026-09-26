import { describe, it, expect, vi } from "vitest";
import {
    CircuitBreaker,
    CircuitState,
    CircuitBreakerOpenException,
} from "../circuit-breaker.js";

describe("CircuitBreaker", () => {
    it("should start in CLOSED state and execute actions normally", async () => {
        const breaker = new CircuitBreaker({ name: "TestService" });
        expect(breaker.getState()).toBe(CircuitState.CLOSED);

        const result = await breaker.execute(async () => "OK");
        expect(result).toBe("OK");
        expect(breaker.getState()).toBe(CircuitState.CLOSED);
    });

    it("should trip to OPEN when failure ratio exceeds threshold", async () => {
        const breaker = new CircuitBreaker({
            name: "TestService",
            minimumRequests: 4,
            failureThresholdRatio: 0.5,
            samplingPeriodMs: 5000,
            cooldownPeriodMs: 1000,
        });

        // 1 success, 3 failures = 75% failure rate >= 50%
        await breaker.execute(async () => "OK");
        for (let i = 0; i < 3; i++) {
            await breaker.execute(async () => {
                throw new Error("Downstream 500");
            }).catch(() => {});
        }

        expect(breaker.getState()).toBe(CircuitState.OPEN);

        // Next call should immediately fail-fast with CircuitBreakerOpenException without invoking action
        const mockAction = vi.fn().mockResolvedValue("NEVER_CALLED");
        await expect(breaker.execute(mockAction)).rejects.toThrow(CircuitBreakerOpenException);
        expect(mockAction).not.toHaveBeenCalled();
    });

    it("should invoke fallback when circuit is OPEN if fallback is provided", async () => {
        const breaker = new CircuitBreaker({
            name: "TestService",
            minimumRequests: 2,
            failureThresholdRatio: 0.5,
            cooldownPeriodMs: 5000,
        });

        // Trigger OPEN
        await breaker.execute(async () => { throw new Error("Fail 1"); }).catch(() => {});
        await breaker.execute(async () => { throw new Error("Fail 2"); }).catch(() => {});
        expect(breaker.getState()).toBe(CircuitState.OPEN);

        const fallback = vi.fn().mockResolvedValue("FALLBACK_DATA");
        const result = await breaker.execute(async () => "SHOULD_FAIL", fallback);

        expect(result).toBe("FALLBACK_DATA");
        expect(fallback).toHaveBeenCalled();
    });

    it("should transition to HALF_OPEN after cooldown and close on success", async () => {
        const breaker = new CircuitBreaker({
            name: "TestService",
            minimumRequests: 2,
            failureThresholdRatio: 0.5,
            cooldownPeriodMs: 50, // 50ms cooldown for fast testing
        });

        // Trip to OPEN
        await breaker.execute(async () => { throw new Error("Fail 1"); }).catch(() => {});
        await breaker.execute(async () => { throw new Error("Fail 2"); }).catch(() => {});
        expect(breaker.getState()).toBe(CircuitState.OPEN);

        // Wait for cooldown period
        await new Promise((r) => setTimeout(r, 60));

        // State transitions to HALF_OPEN
        expect(breaker.getState()).toBe(CircuitState.HALF_OPEN);

        // Probing request succeeds -> Circuit closes
        const probeResult = await breaker.execute(async () => "PROBE_SUCCESS");
        expect(probeResult).toBe("PROBE_SUCCESS");
        expect(breaker.getState()).toBe(CircuitState.CLOSED);
    });
});
