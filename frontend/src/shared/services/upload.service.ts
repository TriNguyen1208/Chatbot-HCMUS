import { mediaApi } from "@/features/chat/api/media.api";
import { retryWithBackoff } from "@/shared/utils/retry.util";

export interface MultipartUploadResult {
    fileKey: string;
    resourceUrl?: string;
}

export enum CircuitState {
    CLOSED = "CLOSED",
    OPEN = "OPEN",
    HALF_OPEN = "HALF_OPEN",
}

interface RequestRecord {
    timestamp: number;
    success: boolean;
}

export class UploadCircuitBreaker {
    private state: CircuitState = CircuitState.CLOSED;
    private records: RequestRecord[] = [];
    private lastStateChangeTime: number = Date.now();

    constructor(
        private readonly failureThresholdRatio = 0.5,
        private readonly minimumRequests = 4,
        private readonly samplingPeriodMs = 10000,
        private readonly cooldownPeriodMs = 30000,
    ) {}

    public getState(): CircuitState {
        this.evaluateState();
        return this.state;
    }

    private evaluateState(): void {
        if (this.state === CircuitState.OPEN) {
            const timeSinceOpen = Date.now() - this.lastStateChangeTime;
            if (timeSinceOpen >= this.cooldownPeriodMs) {
                this.state = CircuitState.HALF_OPEN;
                this.lastStateChangeTime = Date.now();
                console.log(
                    "[UploadCircuitBreaker] 🟡 Transitioned from OPEN -> HALF_OPEN (Probing)",
                );
            }
        }
    }

    private recordResult(success: boolean): void {
        const threshold = Date.now() - this.samplingPeriodMs;
        this.records = this.records.filter((r) => r.timestamp >= threshold);
        this.records.push({ timestamp: Date.now(), success });

        if (this.state === CircuitState.HALF_OPEN) {
            if (success) {
                this.state = CircuitState.CLOSED;
                this.lastStateChangeTime = Date.now();
                this.records = [];
                console.log(
                    "[UploadCircuitBreaker] 🟢 Storage healthy. Transitioned from HALF_OPEN -> CLOSED",
                );
            } else {
                this.state = CircuitState.OPEN;
                this.lastStateChangeTime = Date.now();
                console.warn(
                    "[UploadCircuitBreaker] 🔴 Storage probe failed. Transitioned from HALF_OPEN -> OPEN",
                );
            }
            return;
        }

        if (
            this.state === CircuitState.CLOSED &&
            this.records.length >= this.minimumRequests
        ) {
            const failures = this.records.filter((r) => !r.success).length;
            const ratio = failures / this.records.length;
            if (ratio >= this.failureThresholdRatio) {
                this.state = CircuitState.OPEN;
                this.lastStateChangeTime = Date.now();
                console.error(
                    `[UploadCircuitBreaker] 🔴 R2 Storage failure ratio ${(ratio * 100).toFixed(0)}% ` +
                        `exceeded threshold. Circuit TRIP -> OPEN for ${this.cooldownPeriodMs / 1000}s`,
                );
            }
        }
    }

    public async execute<T>(action: () => Promise<T>): Promise<T> {
        this.evaluateState();

        if (this.state === CircuitState.OPEN) {
            throw new Error(
                "Máy chủ lưu trữ đang bận (Cloudflare R2 không phản hồi), vui lòng thử lại sau ít phút.",
            );
        }

        try {
            const result = await action();
            this.recordResult(true);
            return result;
        } catch (error) {
            this.recordResult(false);
            throw error;
        }
    }
}

export const uploadCircuitBreaker = new UploadCircuitBreaker();

export const uploadService = {
    uploadImage: async (
        file: File,
    ): Promise<{ url: string; fileKey: string }> => {
        const res = await mediaApi.uploadImage(file);
        const resObj = res as { resource_url?: string; url?: string };
        const url = resObj?.resource_url || res.url;
        return { url, fileKey: res.fileKey };
    },

    uploadVideoMultipart: async (
        file: File,
        onProgress?: (percent: number) => void,
    ): Promise<MultipartUploadResult> => {
        const CHUNK_SIZE = 5 * 1024 * 1024; // 5MB chunks
        const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

        const initRes = await mediaApi.initMultipartUpload(
            file.name,
            file.type,
        );
        const { uploadId, fileKey } = initRes;

        const partNumbers = Array.from(
            { length: totalChunks },
            (_, i) => i + 1,
        );
        const urlRes = await mediaApi.getPresignedUrlsForMultipart(
            fileKey,
            uploadId,
            partNumbers,
        );
        const presignedUrls = urlRes.urls;

        const uploadedParts: { ETag: string; PartNumber: number }[] = [];
        let completedChunks = 0;

        const uploadPromises = partNumbers.map(async (partNumber, index) => {
            const start = (partNumber - 1) * CHUNK_SIZE;
            const end = Math.min(start + CHUNK_SIZE, file.size);
            const chunk = file.slice(start, end);
            const presignedUrl = presignedUrls[index];

            const uploadRes = await uploadCircuitBreaker.execute(async () => {
                return await retryWithBackoff(
                    async () => {
                        const res = await fetch(presignedUrl, {
                            method: "PUT",
                            body: chunk,
                        });
                        if (!res.ok) {
                            const err = new Error(
                                `Lỗi tải lên chunk ${partNumber}: HTTP ${res.status}`,
                            );
                            (err as { status?: number }).status = res.status;
                            throw err;
                        }
                        return res;
                    },
                    {
                        maxRetries: 3,
                        baseDelayMs: 1000,
                        maxDelayMs: 8000,
                        shouldRetry: (err: unknown) => {
                            const status = (err as { status?: number })?.status;
                            return status === undefined || status >= 500;
                        },
                        onRetry: (attempt, error, delay) => {
                            console.warn(
                                `[UploadService] ⚠️ Chunk ${partNumber} retry attempt ${attempt} in ${delay}ms:`,
                                error instanceof Error
                                    ? error.message
                                    : String(error),
                            );
                        },
                    },
                );
            });

            const eTag = uploadRes.headers.get("ETag")?.replace(/"/g, "") || "";
            uploadedParts.push({ ETag: eTag, PartNumber: partNumber });

            completedChunks++;
            if (onProgress) {
                onProgress(Math.round((completedChunks / totalChunks) * 100));
            }
        });

        await Promise.all(uploadPromises);

        const completeRes = await mediaApi.completeMultipartUpload(
            fileKey,
            uploadId,
            uploadedParts,
        );
        const resourceUrl =
            completeRes?.data?.resource_url || completeRes?.resource_url;

        return { fileKey, resourceUrl };
    },
};
