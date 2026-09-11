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
}

export const socketService = new SocketService();
