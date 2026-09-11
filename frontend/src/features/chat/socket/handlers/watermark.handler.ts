import { Socket } from "socket.io-client";
import { QueryClient } from "@tanstack/react-query";
import { chatCache } from "../../utils/chat-cache.util";

export const registerWatermarkHandlers = (
  socket: Socket,
  queryClient: QueryClient
) => {
  const onWatermarkUpdated = (data: {
    conversationId: string;
    userId: string;
    messageId: string;
    type: "delivered" | "read";
  }) => {
    chatCache.updateConversationWatermarks(queryClient, data);
  };

  socket.on("watermark_updated", onWatermarkUpdated);

  return () => {
    socket.off("watermark_updated", onWatermarkUpdated);
  };
};
