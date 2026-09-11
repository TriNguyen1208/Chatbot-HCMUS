import { Socket } from "socket.io-client";
import { QueryClient } from "@tanstack/react-query";
import type { Message } from "@/types";
import { chatCache } from "../../utils/chat-cache.util";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useChatStore } from "../../stores/chatStore";

export const registerMessageHandlers = (
  socket: Socket,
  queryClient: QueryClient
) => {
  const onNewMessage = (message: Message) => {
    const { activeConversation } = useChatStore.getState();
    const { user } = useAuthStore.getState();

    // Tự động emit mark_read / mark_delivered nếu tin nhắn từ người khác
    if (user?.id && message.sender_id !== user.id && message.conversation_id && message.id) {
      const isActive = activeConversation?.id === message.conversation_id;
      if (isActive) {
        socket.emit("mark_read", {
          conversationId: message.conversation_id,
          messageId: message.id,
        });
      } else {
        socket.emit("mark_delivered", {
          conversationId: message.conversation_id,
          messageId: message.id,
        });
      }
    }

    chatCache.appendNewMessage(queryClient, message);
    chatCache.bumpConversationLastMessage(queryClient, message);
  };

  const onMessageEdited = (data: {
    conversation_id: string;
    messageId: string;
    content: string;
    updated_at: string;
    edit_history?: { content: string; updated_at: string | Date }[];
  }) => {
    chatCache.updateMessageContent(queryClient, data);
  };

  const onMessageReactionUpdated = (data: {
    conversation_id: string;
    message_id: string;
    reactions: any[];
  }) => {
    chatCache.updateMessageReaction(queryClient, data);
  };

  const onMessageRecalled = (data: {
    conversation_id: string;
    messageId: string;
  }) => {
    chatCache.markMessageRecalled(queryClient, data);
  };

  socket.on("new_message", onNewMessage);
  socket.on("message_edited", onMessageEdited);
  socket.on("message_reaction_updated", onMessageReactionUpdated);
  socket.on("message_recalled", onMessageRecalled);

  return () => {
    socket.off("new_message", onNewMessage);
    socket.off("message_edited", onMessageEdited);
    socket.off("message_reaction_updated", onMessageReactionUpdated);
    socket.off("message_recalled", onMessageRecalled);
  };
};
