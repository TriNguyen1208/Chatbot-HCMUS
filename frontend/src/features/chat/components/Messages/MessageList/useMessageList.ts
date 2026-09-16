"use client";
import { useEffect, useMemo } from "react";
import { useInView } from "react-intersection-observer";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useSearchStore } from "@/features/chat/stores/searchStore";
import { messageApi } from "@/features/chat/api/message.api";
import { useMessagesQuery } from "@/features/chat/hooks/useChatQueries";
import type { Message } from "@/types";
import { useState } from "react";

export const useMessageList = () => {
  const { activeConversation, typingUsers } = useChatStore();
  const { user } = useAuthStore();
  const { targetMessageId, setTargetMessageId } = useSearchStore();

  const [contextMessages, setContextMessages] = useState<Message[] | null>(null);

  const activeConversationId = activeConversation?.id;
  const convId = activeConversationId;

  // Reset context view when switching conversations
  useEffect(() => {
    setContextMessages(null);
  }, [convId]);

  const { data, fetchNextPage, hasNextPage, isFetchingNextPage, isLoading } =
    useMessagesQuery(activeConversationId);

  const { ref, inView } = useInView();

  const rawMessages = data?.pages.flatMap((page) => page) || [];
  const messages = contextMessages || rawMessages;

  const currentTypingUsers = (convId ? (typingUsers[convId] || []) : []).filter(
    (u) => u.userId !== user?.id
  );

  useEffect(() => {
    if (!contextMessages && inView && hasNextPage && !isFetchingNextPage && messages.length > 0) {
      fetchNextPage();
    }
  }, [contextMessages, inView, hasNextPage, isFetchingNextPage, fetchNextPage, messages.length]);

  const watermarksByMessageId = useMemo(() => {
    const result: Record<string, { type: 'delivered' | 'read'; userId: string }[]> = {};
    const rawWatermarks = activeConversation?.watermarks || [];

    // Deduplicate watermarks by userId
    const watermarksMap = new Map<
      string,
      { user_id: string; last_delivered_msg_id?: string | null; last_read_msg_id?: string | null }
    >();
    rawWatermarks.forEach((w) => {
      if (w.user_id && w.user_id !== user?.id) {
        const existing = watermarksMap.get(w.user_id);
        if (!existing) {
          watermarksMap.set(w.user_id, { ...w });
        } else {
          watermarksMap.set(w.user_id, {
            user_id: w.user_id,
            last_delivered_msg_id: w.last_delivered_msg_id || existing.last_delivered_msg_id,
            last_read_msg_id: w.last_read_msg_id || existing.last_read_msg_id,
          });
        }
      }
    });

    const getMsgId = (m: any) => String(m?.id || m?._id || "");

    watermarksMap.forEach((w) => {
      const readIdx = w.last_read_msg_id
        ? messages.findIndex((m) => getMsgId(m) === String(w.last_read_msg_id))
        : -1;

      const deliveredIdx = w.last_delivered_msg_id
        ? messages.findIndex((m) => getMsgId(m) === String(w.last_delivered_msg_id))
        : -1;

      let effectiveDeliveredIdx = deliveredIdx;
      if (readIdx !== -1) {
        effectiveDeliveredIdx =
          effectiveDeliveredIdx === -1 ? readIdx : Math.min(effectiveDeliveredIdx, readIdx);
      }

      messages.forEach((msg, idx) => {
        const msgId = getMsgId(msg);
        if (!msgId) return;
        if (!result[msgId]) result[msgId] = [];

        // 1. Read: only attach avatar to the exact latest message read by this user
        if (readIdx !== -1 && readIdx === idx) {
          if (!result[msgId].some((r) => r.userId === w.user_id && r.type === "read")) {
            result[msgId].push({ type: "read", userId: w.user_id });
          }
        }
        // 2. Delivered: message is at or older than delivered point, but newer than read point
        else if (
          effectiveDeliveredIdx !== -1 &&
          idx >= effectiveDeliveredIdx &&
          (readIdx === -1 || idx < readIdx)
        ) {
          if (!result[msgId].some((r) => r.userId === w.user_id && r.type === "delivered")) {
            result[msgId].push({ type: "delivered", userId: w.user_id });
          }
        }
      });
    });

    return result;
  }, [activeConversation?.watermarks, user?.id, messages]);

  useEffect(() => {
    const fetchContextAndScroll = async () => {
      if (targetMessageId && convId) {
        // 1. Kiểm tra xem tin nhắn đã có sẵn trong danh sách tin nhắn hiện tại chưa
        const isAlreadyLoaded = rawMessages.some(
          (m) => String(m.id || (m as any)._id) === String(targetMessageId)
        );

        if (isAlreadyLoaded) {
          setTimeout(() => {
            const el = document.getElementById(`msg-${targetMessageId}`);
            if (el) {
              el.scrollIntoView({ behavior: "smooth", block: "center" });
              el.classList.add("bg-brand-primary/20", "transition-colors", "duration-500");
              setTimeout(() => {
                el.classList.remove("bg-brand-primary/20");
              }, 3000);
            }
          }, 150);
          setTargetMessageId(null);
          return;
        }

        // 2. Nếu là tin nhắn cũ ở xa, tải ngữ cảnh và lưu vào local state (KHÔNG ghi đè React Query cache)
        try {
          const res = await messageApi.getContextMessages(convId, targetMessageId);
          setContextMessages(res);

          setTimeout(() => {
            const el = document.getElementById(`msg-${targetMessageId}`);
            if (el) {
              el.scrollIntoView({ behavior: "smooth", block: "center" });
              el.classList.add("bg-brand-primary/20", "transition-colors", "duration-500");
              setTimeout(() => {
                el.classList.remove("bg-brand-primary/20");
              }, 3000);
            }
          }, 300);

          setTargetMessageId(null);
        } catch (error) {
          console.error("Failed to load context messages:", error);
        }
      }
    };
    fetchContextAndScroll();
  }, [targetMessageId, convId, rawMessages, setTargetMessageId]);

  return {
    messages,
    isLoadingMessages: isLoading,
    hasMoreMessages: contextMessages ? false : hasNextPage,
    ref,
    currentTypingUsers,
    watermarksByMessageId,
    isViewingContext: Boolean(contextMessages),
    clearContextMessages: () => setContextMessages(null),
  };
};
