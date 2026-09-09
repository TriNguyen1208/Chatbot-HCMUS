import { Conversation } from "@/features/chat/types";
import { DEFAULT_AVATAR } from "@/utils/constants";
import { formatDistanceToNow, isToday, format } from "date-fns";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useUserStore } from "@/features/chat/stores/userStore";
import { useEffect } from "react";

export const useChatCard = (conversation: Conversation) => {
  const { user } = useAuthStore();
  const { users, requestUser } = useUserStore();
  
  // Calculate display name and avatar
  const otherMemberId = conversation.member_ids?.find((m: string) => m !== user?.id) as string;
  const otherMember = users[otherMemberId];

  useEffect(() => {
    if (otherMemberId && !otherMember) {
      requestUser(otherMemberId);
    }
  }, [otherMemberId, otherMember, requestUser]);

  const displayName = conversation.type === 'self' ? "Cloud của tôi" : (conversation.name || otherMember?.name || "Người dùng");
  const displayAvatar = conversation.type === 'self' ? (user?.avatar_url || DEFAULT_AVATAR) : (conversation.avatar_url || otherMember?.avatar_url || DEFAULT_AVATAR);

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
  
  const lastMsg = conversation.last_message;
  console.log(lastMsg?.type)
  const rawLastSenderId = lastMsg?.sender_id;
  const lastMsgSender = users[rawLastSenderId || ''];

  useEffect(() => {
    if (rawLastSenderId && rawLastSenderId !== 'system' && !lastMsgSender) {
      requestUser(rawLastSenderId);
    }
  }, [rawLastSenderId, lastMsgSender, requestUser]);

  if (conversation.type === 'group' && lastMsg && rawLastSenderId !== 'system') {
    const isMe = rawLastSenderId === user?.id;
    const senderName = lastMsgSender?.name || lastMsg?.type == 'system' && "System";
    messagePreview = `${isMe ? "Bạn" : senderName}: ${messagePreview}`;
  }

  const isOnline = conversation.type === 'utu' 
    ? (conversation.block ? false : (otherMember?.is_online || false)) 
    : (conversation.member_ids?.some(id => id !== user?.id && users[id]?.is_online) || false);

  // Check if last message is unread for the current user
  const lastMsgId = lastMsg?.id || (lastMsg as any)?._id;
  const isSentByMe = Boolean(lastMsg && user?.id && lastMsg.sender_id === user.id);
  const myWatermark = conversation.watermarks?.find((w) => w.user_id === user?.id);
  const isRead = Boolean(lastMsgId && myWatermark?.last_read_msg_id === lastMsgId);
  const isUnread = Boolean(lastMsg && !isSentByMe && !isRead);

  return {
    displayName,
    displayAvatar,
    timeDisplay,
    messagePreview,
    isOnline,
    isUnread
  };
};
