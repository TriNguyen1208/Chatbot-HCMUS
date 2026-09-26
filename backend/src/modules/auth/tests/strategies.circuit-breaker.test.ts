import { describe, it, expect, vi, beforeEach } from "vitest";
import { GoogleAuthStrategy } from "../strategies/google.strategy.js";
import { MicrosoftAuthStrategy } from "../strategies/microsoft.strategy.js";
import { CircuitBreaker } from "#@/infrastructure/resilience/circuit-breaker.js";
import type { UserFacade } from "#@/modules/user/user.facade.js";

// Mock google-auth-library
const mockVerifyIdToken = vi.fn();
vi.mock("google-auth-library", () => ({
    OAuth2Client: class {
        verifyIdToken = mockVerifyIdToken;
    }
}));

describe("OAuth Strategies - Circuit Breaker", () => {
    let mockUserFacade: UserFacade;

    beforeEach(() => {
        vi.clearAllMocks();
        mockUserFacade = {} as unknown as UserFacade;
    });

    describe("GoogleAuthStrategy", () => {
        it("should trip circuit breaker and throw 503 ServiceUnavailable when Google Auth API fails repeatedly", async () => {
            const breaker = new CircuitBreaker({
                name: "TestGoogleOAuth",
                failureThresholdRatio: 0.5,
                minimumRequests: 2,
                samplingPeriodMs: 5000,
                cooldownPeriodMs: 10000,
            });

            const strategy = new GoogleAuthStrategy(mockUserFacade, breaker);

            // Giả lập Google API gặp lỗi mạng / 500
            mockVerifyIdToken.mockRejectedValue(new Error("Google Identity Service Unreachable"));

            // Attempt 1: Ném lỗi thông thường từ Google API
            await expect(strategy.authenticate("invalid-token-1")).rejects.toThrow("Google Identity Service Unreachable");

            // Attempt 2: Ném lỗi thông thường (2/2 = 100% fail -> circuit trips to OPEN)
            await expect(strategy.authenticate("invalid-token-2")).rejects.toThrow("Google Identity Service Unreachable");

            expect(breaker.getState()).toBe("OPEN");

            // Attempt 3: Circuit Breaker ngắt mạch OPEN -> Ném ngay 503 ServiceUnavailable mà không gọi Google API nữa!
            const callsBefore = mockVerifyIdToken.mock.calls.length;
            await expect(strategy.authenticate("invalid-token-3")).rejects.toThrow(
                "Dịch vụ xác thực Google tạm thời gián đoạn, vui lòng đăng nhập bằng Email/Mật khẩu."
            );
            expect(mockVerifyIdToken.mock.calls.length).toBe(callsBefore);
        });
    });

    describe("MicrosoftAuthStrategy", () => {
        it("should trip circuit breaker and throw 503 ServiceUnavailable when Microsoft Graph API fails repeatedly", async () => {
            const breaker = new CircuitBreaker({
                name: "TestMicrosoftAuth",
                failureThresholdRatio: 0.5,
                minimumRequests: 2,
                samplingPeriodMs: 5000,
                cooldownPeriodMs: 10000,
            });

            const strategy = new MicrosoftAuthStrategy(mockUserFacade, breaker);

            // Mock fetch toàn cục
            const originalFetch = global.fetch;
            global.fetch = vi.fn().mockRejectedValue(new Error("Microsoft Graph Network Error"));

            try {
                // Attempt 1
                await expect(strategy.authenticate("ms-token-1")).rejects.toThrow("Microsoft Graph Network Error");
                // Attempt 2 (Mạch TRIP to OPEN)
                await expect(strategy.authenticate("ms-token-2")).rejects.toThrow("Microsoft Graph Network Error");

                expect(breaker.getState()).toBe("OPEN");

                // Attempt 3: Fail-fast với 503
                await expect(strategy.authenticate("ms-token-3")).rejects.toThrow(
                    "Dịch vụ xác thực Microsoft tạm thời gián đoạn, vui lòng đăng nhập bằng Email/Mật khẩu."
                );
            } finally {
                global.fetch = originalFetch;
            }
        });
    });
});
