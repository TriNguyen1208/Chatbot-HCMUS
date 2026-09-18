import { Socket } from "socket.io-client";
import { chatCache } from "../../utils/chat-cache.util";

export const registerWatermarkHandlers = (socket: Socket) => {
  const onWatermarkUpdated = (data: {
    conversationId: string;
    userId: string;
    messageId: string;
    type: "delivered" | "read";
  }) => {
    chatCache.updateConversationWatermarks(data);
  };

  socket.on("watermark_updated", onWatermarkUpdated);

  return () => {
    socket.off("watermark_updated", onWatermarkUpdated);
  };
};
