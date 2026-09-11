import { Socket } from "socket.io-client";
import { QueryClient } from "@tanstack/react-query";
import { AppRouterInstance } from "next/dist/shared/lib/app-router-context.shared-runtime";
import type { Conversation } from "@/types";
import { chatCache } from "../../utils/chat-cache.util";
import { useChatStore } from "../../stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { conversationApi } from "@/features/chat/api/conversation.api";

export const registerConversationHandlers = (
  socket: Socket,
  queryClient: QueryClient,
  router: AppRouterInstance
) => {
  const onNewConversation = (conversation: Conversation) => {
    chatCache.addNewConversation(queryClient, conversation);

    // Fix lỗi người dùng tạo đoạn chat mới lần đầu tiên không nhảy URL và ActiveConversation
    const { activeConversation, setActiveConversation } = useChatStore.getState();
    if (activeConversation && !activeConversation.id) {
      if (
        conversation.type === "utu" &&
        conversation.member_ids?.some((m) => m === activeConversation.receiver_id)
      ) {
        setActiveConversation(conversation);
        router.replace(`?conversation_id=${conversation.id}`);
      }
    }
  };

  const onMembersAdded = (data: { conversationId: string; newMemberIds: string[] }) => {
    chatCache.addMembersToConversation(queryClient, data.conversationId, data.newMemberIds);
    queryClient.invalidateQueries({ queryKey: ["conversations"] });

    conversationApi
      .getConversationById(data.conversationId)
      .then((fullConv) => {
        if (fullConv) chatCache.updateConversationInfo(queryClient, fullConv);
      })
      .catch(console.error);
  };

  const onMembersKicked = (data: { conversationId: string; memberIds: string[] }) => {
    chatCache.removeMembersFromConversation(queryClient, data.conversationId, data.memberIds);
    queryClient.invalidateQueries({ queryKey: ["conversations"] });

    const { user } = useAuthStore.getState();
    const { activeConversation, setActiveConversation } = useChatStore.getState();

    if (user?.id && data.memberIds.includes(user.id)) {
      if (activeConversation?.id === data.conversationId) {
        setActiveConversation(null);
        router.push("/group-chat");
      }
    } else {
      conversationApi
        .getConversationById(data.conversationId)
        .then((fullConv) => {
          if (fullConv) chatCache.updateConversationInfo(queryClient, fullConv);
        })
        .catch(console.error);
    }
  };

  const onAdminsUpdated = (data: { conversationId: string; adminIds: string[] }) => {
    chatCache.updateAdminsInConversation(queryClient, data.conversationId, data.adminIds);
    queryClient.invalidateQueries({ queryKey: ["conversations"] });

    conversationApi
      .getConversationById(data.conversationId)
      .then((fullConv) => {
        if (fullConv) chatCache.updateConversationInfo(queryClient, fullConv);
      })
      .catch(console.error);
  };

  const onMemberLeft = (data: { conversationId: string; userId: string }) => {
    chatCache.removeMembersFromConversation(queryClient, data.conversationId, [data.userId]);
    queryClient.invalidateQueries({ queryKey: ["conversations"] });

    const { user } = useAuthStore.getState();
    const { activeConversation, setActiveConversation } = useChatStore.getState();

    if (user?.id && data.userId === user.id) {
      if (activeConversation?.id === data.conversationId) {
        setActiveConversation(null);
        router.push("/group-chat");
      }
    } else {
      conversationApi
        .getConversationById(data.conversationId)
        .then((fullConv) => {
          if (fullConv) chatCache.updateConversationInfo(queryClient, fullConv);
        })
        .catch(console.error);
    }
  };

  const onConversationBlocked = (updatedConv: Conversation) => {
    chatCache.updateConversationBlock(queryClient, updatedConv);
  };

  const onConversationUnblocked = (updatedConv: Conversation) => {
    chatCache.updateConversationBlock(queryClient, { ...updatedConv, block: null });
  };

  const onGroupDisbanded = (data: {
    conversationId: string;
    disbanded_by: string;
    group_name?: string;
  }) => {
    chatCache.removeConversation(queryClient, data.conversationId);

    const { activeConversation, setActiveConversation } = useChatStore.getState();
    if (activeConversation && activeConversation.id === data.conversationId) {
      setActiveConversation(null);
      router.push("/group-chat");
      const { user } = useAuthStore.getState();
      if (user?.id !== data.disbanded_by) {
        alert(`Nhóm "${data.group_name || "này"}" đã bị Quản trị viên giải tán.`);
      }
    }
  };

  const onConversationUpdated = (updatedConv: Conversation) => {
    chatCache.updateConversationInfo(queryClient, updatedConv);
  };

  socket.on("new_conversation", onNewConversation);
  socket.on("members_added", onMembersAdded);
  socket.on("members_kicked", onMembersKicked);
  socket.on("admins_updated", onAdminsUpdated);
  socket.on("member_left", onMemberLeft);
  socket.on("conversation_blocked", onConversationBlocked);
  socket.on("conversation_unblocked", onConversationUnblocked);
  socket.on("group_disbanded", onGroupDisbanded);
  socket.on("conversation_updated", onConversationUpdated);

  return () => {
    socket.off("new_conversation", onNewConversation);
    socket.off("members_added", onMembersAdded);
    socket.off("members_kicked", onMembersKicked);
    socket.off("admins_updated", onAdminsUpdated);
    socket.off("member_left", onMemberLeft);
    socket.off("conversation_blocked", onConversationBlocked);
    socket.off("conversation_unblocked", onConversationUnblocked);
    socket.off("group_disbanded", onGroupDisbanded);
    socket.off("conversation_updated", onConversationUpdated);
  };
};
