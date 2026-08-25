import { Conversation } from "@/features/chat/types";
import { DEFAULT_AVATAR } from "@/utils/constants";
import { formatDistanceToNow, isToday, format } from "date-fns";
import { useAuthStore } from "@/features/auth/stores/authStore";

export const useChatCard = (conversation: Conversation) => {
  const { user } = useAuthStore();
  
  // Calculate display name and avatar
  const otherMember = conversation.members?.find(m => m.id !== user?.id);
  const displayName = conversation.name || otherMember?.name || "Unknown";
  const displayAvatar = conversation.avatar_url || otherMember?.avatar_url || DEFAULT_AVATAR;

  // Calculate time display
  const timeToUse = conversation.last_message?.created_at || conversation.created_at;
  const timeDisplay = timeToUse ? (
    isToday(new Date(timeToUse))
      ? format(new Date(timeToUse), "h:mm a")
      : formatDistanceToNow(new Date(timeToUse), { addSuffix: true })
  ) : "";

  // Calculate message preview
  let messagePreview = conversation.last_message?.content || "";
  if (conversation.last_message) {
    if (conversation.last_message.status === 'recalled') {
      messagePreview = "Tin nhắn đã bị thu hồi";
    } else if (conversation.last_message.type === 'text' || conversation.last_message.type === 'system') {
      messagePreview = conversation.last_message.content ?? '';
    } else {
      messagePreview = `[${conversation.last_message.type}]`;
    }
  }
  const lastSenderId = conversation.last_message?.sender?.id;
  if (conversation.type === 'group' && conversation.last_message?.sender && lastSenderId !== 'system') {
    const isMe = conversation.last_message.sender.id === user?.id;
    messagePreview = `${isMe ? "Bạn" : conversation.last_message.sender.name}: ${messagePreview}`;
  }

  return {
    displayName,
    displayAvatar,
    timeDisplay,
    messagePreview
  };
};
