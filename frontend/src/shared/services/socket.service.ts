import { io, Socket } from "socket.io-client";
import { env } from "@/config/env";

import { retryWithBackoff } from "@/shared/utils/retry.util";

export interface EmitWithAckOptions {
    timeoutMs?: number;
    maxRetries?: number;
    baseDelayMs?: number;
    maxDelayMs?: number;
}

class SocketService {
    private socket: Socket | null = null;

    public connect(): Socket {
        if (!this.socket) {
            this.socket = io(env.apiUrl, {
                withCredentials: true,
                transports: ["websocket"],
                autoConnect: true,
            });

            this.socket.on("connect", () => {
                console.log(
                    "✅ Unified Socket connected with ID:",
                    this.socket?.id,
                );
            });

            this.socket.on("connect_error", (error) => {
                console.error("❌ Unified Socket connection error:", error);
            });
        } else if (!this.socket.connected) {
            this.socket.connect();
        }
        return this.socket;
    }

    public getSocket(): Socket | null {
        return this.socket;
    }

    public isConnected(): boolean {
        return !!this.socket?.connected;
    }

    public disconnect(): void {
        if (this.socket) {
            this.socket.disconnect();
            this.socket = null;
        }
    }

    /**
     * Gửi sự kiện lên Server và chờ phản hồi Acknowledgement (ACK).
     * Tự động kiểm tra kết nối, timeout và tự động retry với Exponential Backoff + Full Jitter
     * khi gặp sự cố mạng hoặc timeout.
     */
    public async emitWithAck<T>(
        event: string,
        data?: unknown,
        optionsOrTimeout: number | EmitWithAckOptions = 5000,
    ): Promise<T> {
        const opts: EmitWithAckOptions =
            typeof optionsOrTimeout === "number"
                ? { timeoutMs: optionsOrTimeout }
                : optionsOrTimeout;

        const {
            timeoutMs = 5000,
            maxRetries = 3,
            baseDelayMs = 1000,
            maxDelayMs = 10000,
        } = opts;

        return retryWithBackoff(
            () => this._emitWithAckOnce<T>(event, data, timeoutMs),
            {
                maxRetries,
                baseDelayMs,
                maxDelayMs,
                shouldRetry: (error: unknown) => {
                    const message =
                        error instanceof Error ? error.message : String(error);
                    const name = error instanceof Error ? error.name : "";
                    // Chỉ retry lỗi timeout hoặc lỗi mất kết nối mạng tạm thời
                    return (
                        name === "TimeoutError" ||
                        message.includes("Timeout") ||
                        message.includes("timed out") ||
                        message.includes("phản hồi quá lâu") ||
                        message.includes("Mất kết nối WebSocket") ||
                        message.includes("Không thể khởi tạo kết nối")
                    );
                },
                onRetry: (attempt, error, delay) => {
                    console.warn(
                        `[SocketService] ⚠️ Retry '${event}' attempt ${attempt} after ${delay}ms due to:`,
                        error instanceof Error ? error.message : error,
                    );
                },
            },
        );
    }

    private async _emitWithAckOnce<T>(
        event: string,
        data?: unknown,
        timeoutMs = 5000,
    ): Promise<T> {
        const socket = this.connect();
        if (!socket) {
            throw new Error("Không thể khởi tạo kết nối WebSocket");
        }

        if (!socket.connected) {
            await new Promise<void>((resolve, reject) => {
                const timer = setTimeout(
                    () =>
                        reject(
                            new Error(
                                "Mất kết nối WebSocket. Vui lòng kiểm tra lại đường truyền mạng.",
                            ),
                        ),
                    3000,
                );
                socket.once("connect", () => {
                    clearTimeout(timer);
                    resolve();
                });
                if (socket.connected) {
                    clearTimeout(timer);
                    resolve();
                }
            });
        }

        try {
            const response = await socket
                .timeout(timeoutMs)
                .emitWithAck(event, data);
            if (!response || typeof response !== "object") {
                return response as T;
            }
            if ("success" in response && !(response as { success: boolean }).success) {
                const msg = (response as { message?: string }).message || "Thao tác thất bại";
                throw new Error(msg);
            }
            const resObj = response as { data?: T };
            return (
                resObj.data !== undefined ? resObj.data : (response as unknown as T)
            );
        } catch (error: unknown) {
            const errObj = error as { message?: string; name?: string };
            if (
                errObj?.message?.includes("operation has timed out") ||
                errObj?.name === "TimeoutError"
            ) {
                throw new Error(
                    "Máy chủ phản hồi quá lâu (Timeout). Vui lòng thử lại.",
                );
            }
            throw error;
        }
    }
}

export const socketService = new SocketService();
