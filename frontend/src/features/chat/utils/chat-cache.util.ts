import { QueryClient } from "@tanstack/react-query";
import type { Message, Conversation, Watermark } from "@/types";
import { useChatStore } from "../stores/chatStore";
import { useUserStore } from "../stores/userStore";

export const chatCache = {
  appendNewMessage: (queryClient: QueryClient, message: Message) => {
    if (!message.conversation_id) return;

    queryClient.setQueryData(
      ["messages", message.conversation_id],
      (oldData: { pages: Message[][]; pageParams: any[] } | undefined) => {
        if (!oldData) {
          return {
            pages: [[message]],
            pageParams: [undefined],
          };
        }

        const newPages = [...oldData.pages];
        let isUpdated = false;

        newPages[0] =
          newPages[0]?.map((m: Message) => {
            if (m.id === message.id) {
              isUpdated = true;
              return message;
            }
            return m;
          }) || [];

        if (!isUpdated) {
          newPages[0] = [message, ...newPages[0]];
        }

        return { ...oldData, pages: newPages };
      }
    );
  },

  bumpConversationLastMessage: (queryClient: QueryClient, message: Message) => {
    if (!message.conversation_id) return;

    let foundInAnyCache = false;

    queryClient.setQueriesData(
      { queryKey: ["conversations"] },
      (oldData: { pages: Conversation[][]; pageParams: any[] } | undefined) => {
        if (!oldData) return oldData;

        let updatedConv: Conversation | null = null;
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

        if (updatedConv && newPages.length > 0) {
          foundInAnyCache = true;
          newPages[0] = [updatedConv, ...newPages[0]];
        }

        return { ...oldData, pages: newPages };
      }
    );

    if (!foundInAnyCache) {
      queryClient.invalidateQueries({ queryKey: ["conversations"] });
    }
  },

  updateMessageContent: (
    queryClient: QueryClient,
    data: {
      conversation_id: string;
      messageId: string;
      content: string;
      updated_at: string;
      edit_history?: { content: string; updated_at: string | Date }[];
    }
  ) => {
    queryClient.setQueryData(
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

    queryClient.setQueriesData(
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
              return {
                ...conv,
                last_message: {
                  ...conv.last_message,
                  content: data.content,
                  updated_at: data.updated_at,
                  is_edited: true,
                  edit_history: data.edit_history,
                },
              };
            }
            return conv;
          })
        );
        return { ...oldData, pages: newPages };
      }
    );
  },

  updateMessageReaction: (
    queryClient: QueryClient,
    data: { conversation_id: string; message_id: string; reactions: any[] }
  ) => {
    queryClient.setQueryData(
      ["messages", data.conversation_id],
      (oldData: { pages: Message[][]; pageParams: any[] } | undefined) => {
        if (!oldData) return oldData;
        const newPages = oldData.pages.map((page: Message[]) =>
          page.map((msg: Message) =>
            msg.id === data.message_id
              ? { ...msg, reactions: data.reactions }
              : msg
          )
        );
        return { ...oldData, pages: newPages };
      }
    );

    queryClient.setQueriesData(
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
              return {
                ...conv,
                last_message: { ...conv.last_message, reactions: data.reactions },
              };
            }
            return conv;
          })
        );
        return { ...oldData, pages: newPages };
      }
    );
  },

  markMessageRecalled: (
    queryClient: QueryClient,
    data: { conversation_id: string; messageId: string }
  ) => {
    queryClient.setQueryData(
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

    queryClient.setQueriesData(
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
              return {
                ...conv,
                last_message: { ...conv.last_message, status: "recalled" },
              };
            }
            return conv;
          })
        );
        return { ...oldData, pages: newPages };
      }
    );
  },

  addNewConversation: (queryClient: QueryClient, conversation: Conversation) => {
    const updateCache = (queryKey: string[]) => {
      queryClient.setQueryData(
        queryKey,
        (oldData: { pages: Conversation[][]; pageParams: any[] } | undefined) => {
          if (!oldData) {
            return { pages: [[conversation]], pageParams: [undefined] };
          }
          const newPages = [...oldData.pages];
          const exists = newPages[0]?.some((c: Conversation) => c.id === conversation.id);
          if (exists) return oldData;

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

  updateConversationWatermarks: (
    queryClient: QueryClient,
    data: { conversationId: string; userId: string; messageId: string; type: "delivered" | "read" }
  ) => {
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
      const existing = map.get(targetUid);
      map.set(targetUid, {
        user_id: targetUid,
        last_delivered_msg_id:
          data.type === "delivered" ? data.messageId : existing?.last_delivered_msg_id || null,
        last_read_msg_id:
          data.type === "read" ? data.messageId : existing?.last_read_msg_id || null,
      });

      return Array.from(map.values());
    };

    queryClient.setQueriesData(
      { queryKey: ["conversations"] },
      (oldData: { pages: Conversation[][]; pageParams: any[] } | undefined) => {
        if (!oldData) return oldData;
        const newPages = oldData.pages.map((page: Conversation[]) =>
          page.map((conv: Conversation) => {
            if (conv.id === data.conversationId) {
              const newWatermarks = mergeWatermarks(conv.watermarks);
              return { ...conv, watermarks: newWatermarks };
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

  updateConversationBlock: (queryClient: QueryClient, updatedConv: Conversation) => {
    queryClient.setQueriesData(
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

  removeConversation: (queryClient: QueryClient, conversationId: string) => {
    queryClient.setQueriesData(
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

  updateConversationInfo: (queryClient: QueryClient, updatedConv: Conversation) => {
    queryClient.setQueriesData(
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

  addMembersToConversation: (
    queryClient: QueryClient,
    conversationId: string,
    newMemberIds: string[]
  ) => {
    queryClient.setQueriesData(
      { queryKey: ["conversations"] },
      (oldData: { pages: Conversation[][]; pageParams: any[] } | undefined) => {
        if (!oldData) return oldData;
        const newPages = oldData.pages.map((page: Conversation[]) =>
          page.map((conv: Conversation) => {
            if (conv.id === conversationId) {
              const currentMembers = conv.member_ids || [];
              const combined = Array.from(new Set([...currentMembers, ...newMemberIds]));
              return { ...conv, member_ids: combined };
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

    newMemberIds.forEach((id) => {
      useUserStore.getState().requestUser(id);
    });
  },

  removeMembersFromConversation: (
    queryClient: QueryClient,
    conversationId: string,
    removedMemberIds: string[]
  ) => {
    queryClient.setQueriesData(
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
              return { ...conv, member_ids: updatedMembers, admin_ids: updatedAdmins };
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

  updateAdminsInConversation: (
    queryClient: QueryClient,
    conversationId: string,
    newAdminIds: string[]
  ) => {
    queryClient.setQueriesData(
      { queryKey: ["conversations"] },
      (oldData: { pages: Conversation[][]; pageParams: any[] } | undefined) => {
        if (!oldData) return oldData;
        const newPages = oldData.pages.map((page: Conversation[]) =>
          page.map((conv: Conversation) => {
            if (conv.id === conversationId) {
              const currentAdmins = conv.admin_ids || [];
              const combined = Array.from(new Set([...currentAdmins, ...newAdminIds]));
              return { ...conv, admin_ids: combined };
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
