import { io, Socket } from "socket.io-client";
import { env } from "@/config/env";

class SocketService {
  private socket: Socket | null = null;

  public connect(): Socket {
    if (!this.socket) {
      this.socket = io(env.apiUrl, {
        withCredentials: true,
        transports: ["websocket", "polling"],
        autoConnect: true,
      });

      this.socket.on("connect", () => {
        console.log("✅ Unified Socket connected with ID:", this.socket?.id);
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
   * Tự động kiểm tra kết nối, timeout 5s và bóc tách dữ liệu { success, data, message }.
   */
  public async emitWithAck<T>(event: string, data?: any, timeoutMs = 5000): Promise<T> {
    const socket = this.connect();
    if (!socket) {
      throw new Error("Không thể khởi tạo kết nối WebSocket");
    }

    if (!socket.connected) {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("Mất kết nối WebSocket. Vui lòng kiểm tra lại đường truyền mạng.")), 3000);
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
      const response = await socket.timeout(timeoutMs).emitWithAck(event, data);
      if (!response || typeof response !== "object") {
        return response as T;
      }
      if ("success" in response && !response.success) {
        throw new Error(response.message || "Thao tác thất bại");
      }
      return (response.data !== undefined ? response.data : response) as T;
    } catch (error: any) {
      if (error?.message?.includes("operation has timed out") || error?.name === "TimeoutError") {
        throw new Error("Máy chủ phản hồi quá lâu (Timeout). Vui lòng thử lại.");
      }
      throw error;
    }
  }
}

export const socketService = new SocketService();
