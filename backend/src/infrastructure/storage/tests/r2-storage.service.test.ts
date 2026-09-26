import { describe, it, expect, vi, beforeEach } from "vitest";
import { CloudflareR2Storage } from "../r2-storage.service.js";
import { CircuitBreaker, CircuitBreakerOpenException } from "#@/infrastructure/resilience/circuit-breaker.js";

// Mock @aws-sdk/client-s3
const mockSend = vi.fn();
vi.mock("@aws-sdk/client-s3", () => {
    return {
        S3Client: class {
            send = mockSend;
        },
        PutObjectCommand: class {
            constructor(public params: any) {}
        },
        GetObjectCommand: class {
            constructor(public params: any) {}
        },
        DeleteObjectCommand: class {},
        CreateMultipartUploadCommand: class {},
        UploadPartCommand: class {},
        CompleteMultipartUploadCommand: class {},
    };
});

describe("CloudflareR2Storage - Circuit Breaker", () => {
    let storage: CloudflareR2Storage;
    let testCircuitBreaker: CircuitBreaker;

    beforeEach(() => {
        vi.clearAllMocks();
        testCircuitBreaker = new CircuitBreaker({
            name: "TestR2Circuit",
            failureThresholdRatio: 0.5,
            minimumRequests: 3,
            samplingPeriodMs: 5000,
            cooldownPeriodMs: 10000,
        });
        storage = new CloudflareR2Storage(testCircuitBreaker);
    });

    it("should trip to OPEN and fast-fail when R2 repeatedly fails", async () => {
        // Giả lập Cloudflare R2 bị lỗi mạng / downtime 500
        mockSend.mockRejectedValue(new Error("R2 503 Service Unavailable"));

        const buffer = Buffer.from("test image");

        // Request 1: thất bại
        await expect(storage.uploadImage(buffer, "img1.png", "image/png")).rejects.toThrow("R2 503 Service Unavailable");
        // Request 2: thất bại
        await expect(storage.uploadImage(buffer, "img2.png", "image/png")).rejects.toThrow("R2 503 Service Unavailable");
        // Request 3: thất bại (3/3 = 100% >= 50% threshold)
        await expect(storage.uploadImage(buffer, "img3.png", "image/png")).rejects.toThrow("R2 503 Service Unavailable");

        // Circuit Breaker lúc này đã TRIP sang OPEN
        expect(testCircuitBreaker.getState()).toBe("OPEN");

        // Request 4: Phải fail-fast ngay lập tức với CircuitBreakerOpenException mà KHÔNG gọi mockSend thêm lần nào!
        const initialCallCount = mockSend.mock.calls.length;
        await expect(storage.uploadImage(buffer, "img4.png", "image/png")).rejects.toThrow(CircuitBreakerOpenException);
        expect(mockSend.mock.calls.length).toBe(initialCallCount);
    });
});
