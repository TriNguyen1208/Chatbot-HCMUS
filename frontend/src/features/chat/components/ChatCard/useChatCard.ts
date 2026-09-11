"use client";
import { Conversation } from "@/types";
import { DEFAULT_AVATAR } from "@/config/constants";
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
      messagePreview = `[Đã gửi ${conversation.last_message.type === 'image' ? 'hình ảnh' : conversation.last_message.type === 'video' ? 'video' : 'tệp tin'}]`;
    }
  }

  const lastMsg = conversation.last_message;
  const rawLastSenderId = lastMsg?.sender_id;
  const lastMsgSender = users[rawLastSenderId || ''];

  useEffect(() => {
    if (rawLastSenderId && rawLastSenderId !== 'system' && !lastMsgSender) {
      requestUser(rawLastSenderId);
    }
  }, [rawLastSenderId, lastMsgSender, requestUser]);

  if (lastMsg && rawLastSenderId && rawLastSenderId !== 'system' && lastMsg.status !== 'recalled') {
    const isMe = rawLastSenderId === user?.id;
    let senderName = "";
    if (isMe) {
      senderName = "Bạn";
    } else if (conversation.type === 'utu') {
      senderName = otherMember?.name || displayName;
    } else {
      senderName = lastMsgSender?.name || "Thành viên";
    }
    messagePreview = `${senderName}: ${messagePreview}`;
  }

  // Calculate online status
  const isOnline = conversation.type === 'self'
    ? false
    : conversation.type === 'utu'
      ? (conversation.block ? false : (otherMember?.is_online || false))
      : (conversation.member_ids?.some((id: string) => id !== user?.id && users[id]?.is_online) || false);

  // Calculate unread status
  const myWatermark = conversation.watermarks?.find(w => w.user_id === user?.id);
  const lastReadId = myWatermark?.last_read_msg_id;
  const lastMsgId = conversation.last_message?.id;
  const isLastMsgFromMe = conversation.last_message?.sender_id === user?.id;

  const isUnread = Boolean(lastMsgId && !isLastMsgFromMe && lastReadId !== lastMsgId);

  return {
    displayName,
    displayAvatar,
    timeDisplay,
    messagePreview,
    isOnline,
    isUnread
  };
};
