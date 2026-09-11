import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { useSocketContext } from "@/providers/SocketProvider";
import {
  registerMessageHandlers,
  registerConversationHandlers,
  registerWatermarkHandlers,
  registerPresenceHandlers,
  registerTypingHandlers,
} from "./handlers";

export const useChatSocket = () => {
  const { socket, isConnected } = useSocketContext();
  const queryClient = useQueryClient();
  const router = useRouter();

  useEffect(() => {
    if (!socket || !isConnected) return;

    // Heartbeat ping with jitter
    const pingInterval = setInterval(() => {
      const jitter = Math.random() * 2000;
      setTimeout(() => {
        if (socket.connected) {
          socket.emit("ping");
        }
      }, jitter);
    }, 29000);

    // Register all socket handlers and collect their cleanup functions
    const cleanups = [
      registerMessageHandlers(socket, queryClient),
      registerConversationHandlers(socket, queryClient, router),
      registerWatermarkHandlers(socket, queryClient),
      registerPresenceHandlers(socket),
      registerTypingHandlers(socket),
    ];

    return () => {
      clearInterval(pingInterval);
      cleanups.forEach((cleanup) => cleanup());
    };
  }, [socket, isConnected, queryClient, router]);

  return socket;
};
