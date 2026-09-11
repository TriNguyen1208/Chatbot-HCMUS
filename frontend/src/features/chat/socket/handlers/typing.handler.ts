import { Socket } from "socket.io-client";
import { useChatStore } from "../../stores/chatStore";

export const registerTypingHandlers = (socket: Socket) => {
  const onTyping = (data: { conversationId: string; userId: string; name: string }) => {
    useChatStore.getState().addTypingUser(data.conversationId, data.userId, data.name);
  };

  const onStopTyping = (data: { conversationId: string; userId: string }) => {
    useChatStore.getState().removeTypingUser(data.conversationId, data.userId);
  };

  socket.on("typing", onTyping);
  socket.on("stop_typing", onStopTyping);

  return () => {
    socket.off("typing", onTyping);
    socket.off("stop_typing", onStopTyping);
  };
};
