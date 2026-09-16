import { Socket } from "socket.io-client";
import { QueryClient } from "@tanstack/react-query";
import { AppRouterInstance } from "next/dist/shared/lib/app-router-context.shared-runtime";
import type { Conversation } from "@/types";
import { chatCache } from "../../utils/chat-cache.util";
import { useChatStore } from "../../stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
export const registerConversationHandlers = (
  socket: Socket,
  queryClient: QueryClient,
  router: AppRouterInstance
) => {
  const onNewConversation = (conversation: Conversation) => {
    chatCache.addNewConversation(queryClient, conversation);
    const { activeConversation, setActiveConversation } = useChatStore.getState();
    if (activeConversation && !activeConversation.id) {
      if (
        conversation.type === "utu" &&
        conversation.member_ids?.some((m) => m === activeConversation.receiver_id)
      ) {
        setActiveConversation(conversation);
        const basePath = (typeof window !== 'undefined' && window.location.pathname) || '/chat';
        router.replace(`${basePath}?conversation_id=${conversation.id}`);
      }
    }
  };

  const onMembersAdded = (data: { conversationId: string; newMemberIds: string[] }) => {
    // Cập nhật danh sách thành viên trực tiếp vào cache từ payload socket
    chatCache.addMembersToConversation(queryClient, data.conversationId, data.newMemberIds);
  };

  const onMembersKicked = (data: { conversationId: string; memberIds: string[] }) => {
    const { user } = useAuthStore.getState();
    const { activeConversation, setActiveConversation } = useChatStore.getState();

    if (user?.id && data.memberIds.includes(user.id)) {
      // Chính user bị kick -> Xoá conversation khỏi cache
      chatCache.removeConversation(queryClient, data.conversationId);
      if (activeConversation?.id === data.conversationId) {
        setActiveConversation(null);
      }
    } else {
      // Thành viên khác bị kick -> Cập nhật loại bỏ khỏi cache
      chatCache.removeMembersFromConversation(queryClient, data.conversationId, data.memberIds);
    }
  };

  const onAdminsUpdated = (data: { conversationId: string; adminIds: string[] }) => {
    // Cập nhật danh sách admin trực tiếp vào cache từ payload socket
    chatCache.updateAdminsInConversation(queryClient, data.conversationId, data.adminIds);
  };

  const onMemberLeft = (data: { conversationId: string; userId: string }) => {
    const { user } = useAuthStore.getState();
    const { activeConversation, setActiveConversation } = useChatStore.getState();

    if (user?.id && data.userId === user.id) {
      // Chính user rời nhóm -> Xoá conversation khỏi cache
      chatCache.removeConversation(queryClient, data.conversationId);
      if (activeConversation?.id === data.conversationId) {
        setActiveConversation(null);
        router.push(`/chat`);
      }
    } else {
      // Thành viên khác rời nhóm -> Cập nhật loại bỏ khỏi cache
      chatCache.removeMembersFromConversation(queryClient, data.conversationId, [data.userId]);
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
      const { user } = useAuthStore.getState();
      if (user?.id !== data.disbanded_by) {
        alert(`Nhóm "${data.group_name || "này"}" đã bị Quản trị viên giải tán.`);
      }
      router.push('/chat')
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
