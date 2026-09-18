import { QueryClient } from "@tanstack/react-query";
import { queryClient as defaultQueryClient } from "@/providers/QueryProvider";
import type { Message, Conversation, Watermark } from "@/types";
import { useChatStore } from "../stores/chatStore";
import { useUserStore } from "../stores/userStore";
import { conversationApi } from "../api/conversation.api";

// Fast memory indexes for O(1) lookups
const directIndex = new Map<string, string>(); // receiverId -> conversationId
let cachedSelfConversationId: string | null = null;

function resolveArgs(args: any[]): { client: QueryClient; actualArgs: any[] } {
  if (args[0] && typeof args[0].getQueryData === "function") {
    return { client: args[0] as QueryClient, actualArgs: args.slice(1) };
  }
  if (args[0] === undefined && args.length > 1) {
    return { client: defaultQueryClient, actualArgs: args.slice(1) };
  }
  return { client: defaultQueryClient, actualArgs: args };
}

export const chatCache = {
  /**
   * Prime or index conversations in memory and individual cache for O(1) lookups.
   */
  indexConversations: (
    conversations: Conversation[],
    currentUserId?: string,
    client: QueryClient = defaultQueryClient
  ) => {
    for (const conv of conversations) {
      if (!conv?.id) continue;
      // 1. Prime direct query cache for O(1) lookup by conversationId
      client.setQueryData(["conversation", conv.id], conv);

      // 2. Index self conversation
      if (conv.type === "self") {
        cachedSelfConversationId = conv.id;
      }

      // 3. Index direct 1-1 conversation by receiverId
      if (conv.type === "utu" && Array.isArray(conv.member_ids)) {
        const otherId = conv.member_ids.find((m) => m !== currentUserId);
        if (otherId) {
          directIndex.set(otherId, conv.id);
        }
      }
    }
  },

  /**
   * O(1) lookup for a conversation by ID.
   */
  getConversation: (conversationId: string, client: QueryClient = defaultQueryClient): Conversation | undefined => {
    if (!conversationId) return undefined;

    // 1. Direct query lookup O(1)
    const direct = client.getQueryData<Conversation>(["conversation", conversationId]);
    if (direct) return direct;

    // 2. Scan ['conversations'] cache once and prime individual cache
    const allCaches = client.getQueriesData<{ pages: Conversation[][] }>({ queryKey: ["conversations"] });
    for (const [_, data] of allCaches) {
      if (data?.pages) {
        for (const page of data.pages) {
          const match = page.find((c) => c.id === conversationId);
          if (match) {
            client.setQueryData(["conversation", conversationId], match);
            return match;
          }
        }
      }
    }
    return undefined;
  },

  /**
   * O(1) update/save conversation in cache.
   */
  setConversation: (conversation: Conversation, client: QueryClient = defaultQueryClient): void => {
    if (!conversation?.id) return;
    client.setQueryData(["conversation", conversation.id], conversation);

    if (conversation.type === "self") {
      cachedSelfConversationId = conversation.id;
    }

    if (conversation.type === "utu" && Array.isArray(conversation.member_ids)) {
      const currentUserId = useChatStore.getState().activeConversation?.receiver_id;
      const otherId = conversation.member_ids.find((m) => m !== currentUserId);
      if (otherId) directIndex.set(otherId, conversation.id);
    }
  },

  /**
   * O(1) lookup for 1-1 conversation ID by friend user ID.
   */
  getDirectConversationId: (
    receiverId: string,
    currentUserId?: string,
    client: QueryClient = defaultQueryClient
  ): string | undefined => {
    if (!receiverId) return undefined;

    // 1. Direct memory lookup O(1)
    const indexed = directIndex.get(receiverId);
    if (indexed) return indexed;

    // 2. Scan once and index
    const allCaches = client.getQueriesData<{ pages: Conversation[][] }>({ queryKey: ["conversations"] });
    for (const [_, data] of allCaches) {
      if (data?.pages) {
        for (const page of data.pages) {
          for (const c of page) {
            if (c.id && c.type === "utu" && c.member_ids?.includes(receiverId)) {
              if (!currentUserId || c.member_ids.includes(currentUserId)) {
                directIndex.set(receiverId, c.id);
                client.setQueryData(["conversation", c.id], c);
                return c.id;
              }
            }
          }
        }
      }
    }
    return undefined;
  },

  /**
   * O(1) lookup for self conversation ID.
   */
  getSelfConversationId: (
    currentUserId?: string,
    client: QueryClient = defaultQueryClient
  ): string | undefined => {
    if (cachedSelfConversationId) return cachedSelfConversationId;

    const allCaches = client.getQueriesData<{ pages: Conversation[][] }>({ queryKey: ["conversations"] });
    for (const [_, data] of allCaches) {
      if (data?.pages) {
        for (const page of data.pages) {
          for (const c of page) {
            if (c.id && c.type === "self") {
              if (!currentUserId || c.member_ids?.includes(currentUserId)) {
                cachedSelfConversationId = c.id;
                client.setQueryData(["conversation", c.id], c);
                return c.id;
              }
            }
          }
        }
      }
    }
    return undefined;
  },

  invalidateConversations: (client: QueryClient = defaultQueryClient) => {
    client.invalidateQueries({ queryKey: ["conversations"] });
  },

  appendNewMessage: (...args: any[]) => {
    const { client, actualArgs } = resolveArgs(args);
    const message: Message = actualArgs[0];
    if (!message || !message.conversation_id) return;

    client.setQueryData(
      ["messages", message.conversation_id],
      (oldData: { pages: Message[][]; pageParams: any[] } | undefined) => {
        if (!oldData) {
          return {
            pages: [[message]],
            pageParams: [undefined],
          };
        }

        const msgId = message.id || (message as any)._id;

        // Dedup: Remove any existing instance of this message across ALL pages
        const newPages = oldData.pages.map((page: Message[]) =>
          page.filter((m: Message) => {
            const id = m.id || (m as any)._id;
            return id !== msgId;
          })
        );

        // Prepend to newest page (index 0)
        newPages[0] = [message, ...(newPages[0] || [])];

        return { ...oldData, pages: newPages };
      }
    );
  },

  bumpConversationLastMessage: (...args: any[]) => {
    const { client, actualArgs } = resolveArgs(args);
    const message: Message = actualArgs[0];
    if (!message || !message.conversation_id) return;

    let foundInAnyCache = false;

    client.setQueriesData(
      { queryKey: ["conversations"] },
      (oldData: { pages: Conversation[][]; pageParams: any[] } | undefined) => {
        if (!oldData) return oldData;

        let updatedConv: Conversation | null = null;
        // Dedup: Filter out this conversation across ALL pages
        const newPages = oldData.pages.map((page: Conversation[]) => {
          return page.filter((conv: Conversation) => {
            if (conv.id === message.conversation_id) {
              updatedConv = {
                ...conv,
                last_message: message,
                created_at: conv.created_at,
              };
              return false;
            }
            return true;
          });
        });

        if (updatedConv) {
          foundInAnyCache = true;
          newPages[0] = [updatedConv, ...(newPages[0] || [])];
          client.setQueryData(["conversation", (updatedConv as Conversation).id], updatedConv);
        }

        return { ...oldData, pages: newPages };
      }
    );

    if (!foundInAnyCache) {
      // Thay vì invalidate làm refetch toàn bộ danh sách:
      // 1. Kiểm tra xem conversation đã có trong cache đơn lẻ ['conversation', id] chưa
      const cached = client.getQueryData<Conversation>(["conversation", message.conversation_id]);
      if (cached) {
        const updated = {
          ...cached,
          last_message: message,
        };
        chatCache.addNewConversation(updated, client);
      } else {
        // 2. Fetch đúng 1 conversation này từ server và chèn vào đầu cache (giữ nguyên toàn bộ các trang khác)
        conversationApi.getConversationById(message.conversation_id).then((conv) => {
          if (conv) {
            const updated = {
              ...conv,
              last_message: message,
            };
            chatCache.addNewConversation(updated, client);
          }
        }).catch((err) => {
          console.error("Failed to fetch single conversation for cache bump:", err);
        });
      }
    }
  },

  updateMessageContent: (...args: any[]) => {
    const { client, actualArgs } = resolveArgs(args);
    const data: {
      conversation_id: string;
      messageId: string;
      content: string;
      updated_at: string;
      edit_history?: { content: string; updated_at: string | Date }[];
    } = actualArgs[0];
    if (!data?.conversation_id) return;

    client.setQueryData(
      ["messages", data.conversation_id],
      (oldData: { pages: Message[][]; pageParams: any[] } | undefined) => {
        if (!oldData) return oldData;
        const newPages = oldData.pages.map((page: Message[]) =>
          page.map((msg: Message) =>
            msg.id === data.messageId
              ? {
                  ...msg,
                  content: data.content,
                  updated_at: data.updated_at,
                  is_edited: true,
                  edit_history: data.edit_history,
                }
              : msg
          )
        );
        return { ...oldData, pages: newPages };
      }
    );

    // Cập nhật last_message trong conversation nếu đây là tin nhắn cuối
    client.setQueriesData(
      { queryKey: ["conversations"] },
      (oldData: { pages: Conversation[][]; pageParams: any[] } | undefined) => {
        if (!oldData) return oldData;
        const newPages = oldData.pages.map((page: Conversation[]) =>
          page.map((conv: Conversation) => {
            if (
              conv.id === data.conversation_id &&
              conv.last_message &&
              conv.last_message.id === data.messageId
            ) {
              const updated = {
                ...conv,
                last_message: {
                  ...conv.last_message,
                  content: data.content,
                  updated_at: data.updated_at,
                  is_edited: true,
                  edit_history: data.edit_history,
                },
              };
              client.setQueryData(["conversation", conv.id], updated);
              return updated;
            }
            return conv;
          })
        );
        return { ...oldData, pages: newPages };
      }
    );
  },

  updateMessageReaction: (...args: any[]) => {
    const { client, actualArgs } = resolveArgs(args);
    const data: { conversation_id: string; message_id: string; reactions: any[] } = actualArgs[0];
    if (!data?.conversation_id) return;

    client.setQueryData(
      ["messages", data.conversation_id],
      (oldData: { pages: Message[][]; pageParams: any[] } | undefined) => {
        if (!oldData) return oldData;
        const newPages = oldData.pages.map((page: Message[]) =>
          page.map((msg: Message) =>
            msg.id === data.message_id ? { ...msg, reactions: data.reactions } : msg
          )
        );
        return { ...oldData, pages: newPages };
      }
    );

    client.setQueriesData(
      { queryKey: ["conversations"] },
      (oldData: { pages: Conversation[][]; pageParams: any[] } | undefined) => {
        if (!oldData) return oldData;
        const newPages = oldData.pages.map((page: Conversation[]) =>
          page.map((conv: Conversation) => {
            if (
              conv.id === data.conversation_id &&
              conv.last_message &&
              conv.last_message.id === data.message_id
            ) {
              const updated = {
                ...conv,
                last_message: { ...conv.last_message, reactions: data.reactions },
              };
              client.setQueryData(["conversation", conv.id], updated);
              return updated;
            }
            return conv;
          })
        );
        return { ...oldData, pages: newPages };
      }
    );
  },

  markMessageRecalled: (...args: any[]) => {
    const { client, actualArgs } = resolveArgs(args);
    const data: { conversation_id: string; messageId: string } = actualArgs[0];
    if (!data?.conversation_id) return;

    client.setQueryData(
      ["messages", data.conversation_id],
      (oldData: { pages: Message[][]; pageParams: any[] } | undefined) => {
        if (!oldData) return oldData;
        const newPages = oldData.pages.map((page: Message[]) =>
          page.map((msg: Message) =>
            msg.id === data.messageId ? { ...msg, status: "recalled" } : msg
          )
        );
        return { ...oldData, pages: newPages };
      }
    );

    client.setQueriesData(
      { queryKey: ["conversations"] },
      (oldData: { pages: Conversation[][]; pageParams: any[] } | undefined) => {
        if (!oldData) return oldData;
        const newPages = oldData.pages.map((page: Conversation[]) =>
          page.map((conv: Conversation) => {
            if (
              conv.id === data.conversation_id &&
              conv.last_message &&
              conv.last_message.id === data.messageId
            ) {
              const updated = {
                ...conv,
                last_message: { ...conv.last_message, status: "recalled" as const },
              };
              client.setQueryData(["conversation", conv.id], updated);
              return updated;
            }
            return conv;
          })
        );
        return { ...oldData, pages: newPages };
      }
    );
  },

  addNewConversation: (...args: any[]) => {
    const { client, actualArgs } = resolveArgs(args);
    const conversation: Conversation = actualArgs[0];
    if (!conversation?.id) return;

    // 1. Prime individual cache for O(1) lookup
    client.setQueryData(["conversation", conversation.id], conversation);

    if (conversation.type === "self") {
      cachedSelfConversationId = conversation.id;
    }

    const updateCache = (queryKey: string[]) => {
      client.setQueryData(
        queryKey,
        (oldData: { pages: Conversation[][]; pageParams: any[] } | undefined) => {
          if (!oldData) {
            return { pages: [[conversation]], pageParams: [undefined] };
          }
          // Dedup: filter out across ALL pages before prepending
          const newPages = oldData.pages.map((page: Conversation[]) =>
            page.filter((c: Conversation) => c.id !== conversation.id)
          );
          newPages[0] = [conversation, ...(newPages[0] || [])];
          return { ...oldData, pages: newPages };
        }
      );
    };

    updateCache(["conversations"]);
    if (conversation.type) {
      updateCache(["conversations", conversation.type]);
    }
  },

  updateConversationWatermarks: (...args: any[]) => {
    const { client, actualArgs } = resolveArgs(args);
    const data: {
      conversationId: string;
      userId: string;
      messageId: string;
      type: "delivered" | "read";
    } = actualArgs[0];
    if (!data?.conversationId) return;

    const mergeWatermarks = (currentWatermarks: Watermark[] = []): Watermark[] => {
      const map = new Map<string, Watermark>();

      for (const w of currentWatermarks) {
        if (!w || !w.user_id) continue;
        const uid = String(w.user_id);
        const prev = map.get(uid);
        if (!prev) {
          map.set(uid, {
            user_id: uid,
            last_delivered_msg_id: w.last_delivered_msg_id || null,
            last_read_msg_id: w.last_read_msg_id || null,
          });
        } else {
          map.set(uid, {
            user_id: uid,
            last_delivered_msg_id: w.last_delivered_msg_id || prev.last_delivered_msg_id || null,
            last_read_msg_id: w.last_read_msg_id || prev.last_read_msg_id || null,
          });
        }
      }

      const targetUid = String(data.userId);
      const existing = map.get(targetUid) || {
        user_id: targetUid,
        last_delivered_msg_id: null,
        last_read_msg_id: null,
      };

      if (data.type === "delivered") {
        existing.last_delivered_msg_id = data.messageId;
      } else if (data.type === "read") {
        existing.last_read_msg_id = data.messageId;
        existing.last_delivered_msg_id = data.messageId;
      }
      map.set(targetUid, existing);

      return Array.from(map.values());
    };

    client.setQueriesData(
      { queryKey: ["conversations"] },
      (oldData: { pages: Conversation[][]; pageParams: any[] } | undefined) => {
        if (!oldData) return oldData;
        const newPages = oldData.pages.map((page: Conversation[]) =>
          page.map((conv: Conversation) => {
            if (conv.id === data.conversationId) {
              const newWatermarks = mergeWatermarks(conv.watermarks);
              const updated = { ...conv, watermarks: newWatermarks };
              client.setQueryData(["conversation", conv.id], updated);
              return updated;
            }
            return conv;
          })
        );
        return { ...oldData, pages: newPages };
      }
    );

    const { activeConversation, setActiveConversation } = useChatStore.getState();
    if (activeConversation && activeConversation.id === data.conversationId) {
      const newWatermarks = mergeWatermarks(activeConversation.watermarks);
      setActiveConversation({ ...activeConversation, watermarks: newWatermarks });
    }
  },

  updateConversationBlock: (...args: any[]) => {
    const { client, actualArgs } = resolveArgs(args);
    const updatedConv: Conversation = actualArgs[0];
    if (!updatedConv?.id) return;

    client.setQueryData(["conversation", updatedConv.id], updatedConv);

    client.setQueriesData(
      { queryKey: ["conversations"] },
      (oldData: { pages: Conversation[][]; pageParams: any[] } | undefined) => {
        if (!oldData) return oldData;
        const newPages = oldData.pages.map((page: Conversation[]) =>
          page.map((conv: Conversation) =>
            conv.id === updatedConv.id ? { ...conv, ...updatedConv } : conv
          )
        );
        return { ...oldData, pages: newPages };
      }
    );

    const { activeConversation, setActiveConversation } = useChatStore.getState();
    if (activeConversation && activeConversation.id === updatedConv.id) {
      setActiveConversation({ ...activeConversation, ...updatedConv });
    }
  },

  removeConversation: (...args: any[]) => {
    const { client, actualArgs } = resolveArgs(args);
    const conversationId: string = actualArgs[0];
    if (!conversationId) return;

    client.removeQueries({ queryKey: ["conversation", conversationId] });

    client.setQueriesData(
      { queryKey: ["conversations"] },
      (oldData: { pages: Conversation[][]; pageParams: any[] } | undefined) => {
        if (!oldData) return oldData;
        const newPages = oldData.pages.map((page: Conversation[]) =>
          page.filter((conv: Conversation) => conv.id !== conversationId)
        );
        return { ...oldData, pages: newPages };
      }
    );
  },

  updateConversationInfo: (...args: any[]) => {
    const { client, actualArgs } = resolveArgs(args);
    const updatedConv: Conversation = actualArgs[0];
    if (!updatedConv?.id) return;

    client.setQueryData(["conversation", updatedConv.id], updatedConv);

    client.setQueriesData(
      { queryKey: ["conversations"] },
      (oldData: { pages: Conversation[][]; pageParams: any[] } | undefined) => {
        if (!oldData) return oldData;
        const newPages = oldData.pages.map((page: Conversation[]) =>
          page.map((conv: Conversation) =>
            conv.id === updatedConv.id ? { ...conv, ...updatedConv } : conv
          )
        );
        return { ...oldData, pages: newPages };
      }
    );

    const { activeConversation, setActiveConversation } = useChatStore.getState();
    if (activeConversation && activeConversation.id === updatedConv.id) {
      setActiveConversation({ ...activeConversation, ...updatedConv });
    }
  },

  addMembersToConversation: (...args: any[]) => {
    const { client, actualArgs } = resolveArgs(args);
    const conversationId: string = actualArgs[0];
    const newMemberIds: string[] = actualArgs[1];
    if (!conversationId) return;

    client.setQueriesData(
      { queryKey: ["conversations"] },
      (oldData: { pages: Conversation[][]; pageParams: any[] } | undefined) => {
        if (!oldData) return oldData;
        const newPages = oldData.pages.map((page: Conversation[]) =>
          page.map((conv: Conversation) => {
            if (conv.id === conversationId) {
              const currentMembers = conv.member_ids || [];
              const combined = Array.from(new Set([...currentMembers, ...newMemberIds]));
              const updated = { ...conv, member_ids: combined };
              client.setQueryData(["conversation", conv.id], updated);
              return updated;
            }
            return conv;
          })
        );
        return { ...oldData, pages: newPages };
      }
    );

    const { activeConversation, setActiveConversation } = useChatStore.getState();
    if (activeConversation && activeConversation.id === conversationId) {
      const currentMembers = activeConversation.member_ids || [];
      const combined = Array.from(new Set([...currentMembers, ...newMemberIds]));
      setActiveConversation({ ...activeConversation, member_ids: combined });
    }

    newMemberIds?.forEach((id) => {
      useUserStore.getState().requestUser(id);
    });
  },

  removeMembersFromConversation: (...args: any[]) => {
    const { client, actualArgs } = resolveArgs(args);
    const conversationId: string = actualArgs[0];
    const removedMemberIds: string[] = actualArgs[1];
    if (!conversationId) return;

    client.setQueriesData(
      { queryKey: ["conversations"] },
      (oldData: { pages: Conversation[][]; pageParams: any[] } | undefined) => {
        if (!oldData) return oldData;
        const newPages = oldData.pages.map((page: Conversation[]) =>
          page.map((conv: Conversation) => {
            if (conv.id === conversationId) {
              const updatedMembers = (conv.member_ids || []).filter(
                (id) => !removedMemberIds.includes(id)
              );
              const updatedAdmins = (conv.admin_ids || []).filter(
                (id) => !removedMemberIds.includes(id)
              );
              const updated = { ...conv, member_ids: updatedMembers, admin_ids: updatedAdmins };
              client.setQueryData(["conversation", conv.id], updated);
              return updated;
            }
            return conv;
          })
        );
        return { ...oldData, pages: newPages };
      }
    );

    const { activeConversation, setActiveConversation } = useChatStore.getState();
    if (activeConversation && activeConversation.id === conversationId) {
      const updatedMembers = (activeConversation.member_ids || []).filter(
        (id) => !removedMemberIds.includes(id)
      );
      const updatedAdmins = (activeConversation.admin_ids || []).filter(
        (id) => !removedMemberIds.includes(id)
      );
      setActiveConversation({
        ...activeConversation,
        member_ids: updatedMembers,
        admin_ids: updatedAdmins,
      });
    }
  },

  updateAdminsInConversation: (...args: any[]) => {
    const { client, actualArgs } = resolveArgs(args);
    const conversationId: string = actualArgs[0];
    const newAdminIds: string[] = actualArgs[1];
    if (!conversationId) return;

    client.setQueriesData(
      { queryKey: ["conversations"] },
      (oldData: { pages: Conversation[][]; pageParams: any[] } | undefined) => {
        if (!oldData) return oldData;
        const newPages = oldData.pages.map((page: Conversation[]) =>
          page.map((conv: Conversation) => {
            if (conv.id === conversationId) {
              const currentAdmins = conv.admin_ids || [];
              const combined = Array.from(new Set([...currentAdmins, ...newAdminIds]));
              const updated = { ...conv, admin_ids: combined };
              client.setQueryData(["conversation", conv.id], updated);
              return updated;
            }
            return conv;
          })
        );
        return { ...oldData, pages: newPages };
      }
    );

    const { activeConversation, setActiveConversation } = useChatStore.getState();
    if (activeConversation && activeConversation.id === conversationId) {
      const currentAdmins = activeConversation.admin_ids || [];
      const combined = Array.from(new Set([...currentAdmins, ...newAdminIds]));
      setActiveConversation({ ...activeConversation, admin_ids: combined });
    }
  },
};
