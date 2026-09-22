import { Socket } from "socket.io-client";
import { AppRouterInstance } from "next/dist/shared/lib/app-router-context.shared-runtime";
import type { Conversation } from "@/types";
import { chatCache } from "../../utils/chat-cache.util";
import { useChatStore } from "../../stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";


export const registerConversationHandlers = (
    socket: Socket,
    router: AppRouterInstance,
) => {
    /**
     * Sự kiện 'new_conversation':
     * Nhận thông báo khi một cuộc hội thoại mới được tạo ra (trực tiếp 1-1, nhóm, hoặc self chat).
     */
    const onNewConversation = (conversation: Conversation) => {
        // 1. Thêm cuộc trò chuyện mới vào cache danh sách sidebar
        chatCache.addNewConversation(conversation);

        const { activeConversation, setActiveConversation } =
            useChatStore.getState();

        // 2. Xử lý trường hợp người dùng đang ở giao diện chat "nháp" (chưa có id thực tế trong DB)
        if (activeConversation && !activeConversation.id) {
            const isMatchingUtu =
                conversation.type === "utu" &&
                conversation.member_ids?.some(
                    (m) => m === activeConversation.receiver_id,
                );
            const isMatchingSelf =
                conversation.type === "self" &&
                activeConversation.type === "self";

            if (isMatchingUtu || isMatchingSelf) {
                // Gán cuộc trò chuyện chính thức vào state đang hoạt động
                setActiveConversation(conversation);
                const basePath =
                    (typeof window !== "undefined" &&
                        window.location.pathname) ||
                    "/chat";
                // Cập nhật lại URL trên thanh địa chỉ mà không reload trang
                router.replace(
                    `${basePath}?conversation_id=${conversation.id}`,
                );
            }
        }
    };

    /**
     * Sự kiện 'members_added':
     * Nhận thông báo khi có một hoặc nhiều thành viên mới được thêm vào nhóm.
     * Cập nhật danh sách member_ids trực tiếp vào cache TanStack Query.
     */
    const onMembersAdded = (data: {
        conversationId: string;
        newMemberIds: string[];
    }) => {
        chatCache.addMembersToConversation(
            data.conversationId,
            data.newMemberIds,
        );
    };

    /**
     * Sự kiện 'members_kicked':
     * Nhận thông báo khi thành viên bị Trưởng nhóm / Quản trị viên xóa khỏi nhóm.
     */
    const onMembersKicked = (data: {
        conversationId: string;
        memberIds: string[];
    }) => {
        const { user } = useAuthStore.getState();
        const { activeConversation, setActiveConversation } =
            useChatStore.getState();

        // Nếu người dùng hiện tại nằm trong danh sách bị kick
        if (user?.id && data.memberIds.includes(user.id)) {
            chatCache.removeConversation(data.conversationId);
            if (activeConversation?.id === data.conversationId) {
                setActiveConversation(null);
                const basePath =
                    (typeof window !== "undefined" &&
                        window.location.pathname) ||
                    "/chat";
                router.replace(`${basePath}`);
            }
        } else {
            chatCache.removeMembersFromConversation(
                data.conversationId,
                data.memberIds,
            );
        }
    };

    /**
     * Sự kiện 'admins_updated':
     * Nhận thông báo khi danh sách Quản trị viên (Admins) của nhóm thay đổi (bổ nhiệm hoặc cách chức).
     */
    const onAdminsUpdated = (data: {
        conversationId: string;
        adminIds: string[];
    }) => {
        chatCache.updateAdminsInConversation(
            data.conversationId,
            data.adminIds,
        );
    };

    /**
     * Sự kiện 'member_left':
     * Nhận thông báo khi có thành viên tự ý rời khỏi nhóm chat.
     */
    const onMemberLeft = (data: { conversationId: string; userId: string }) => {
        const { user } = useAuthStore.getState();
        const { activeConversation, setActiveConversation } =
            useChatStore.getState();

        // Nếu chính người dùng hiện tại vừa thực hiện hành động rời nhóm
        if (user?.id && data.userId === user.id) {
            // Xóa cuộc trò chuyện khỏi cache và điều hướng về trang chat mặc định
            chatCache.removeConversation(data.conversationId);
            if (activeConversation?.id === data.conversationId) {
                setActiveConversation(null);
                const basePath =
                    (typeof window !== "undefined" &&
                        window.location.pathname) ||
                    "/chat";
                router.replace(`${basePath}`);
            }
        } else {
            // Nếu thành viên khác rời nhóm -> Cập nhật loại bỏ họ khỏi danh sách thành viên của nhóm
            chatCache.removeMembersFromConversation(data.conversationId, [
                data.userId,
            ]);
        }
    };

    /**
     * Sự kiện 'conversation_blocked':
     * Nhận thông báo khi một người trong cuộc trò chuyện 1-1 chặn người kia.
     * Cập nhật thông tin block (blocked_by, created_at...) vào cache.
     */
    const onConversationBlocked = (updatedConv: Conversation) => {
        chatCache.updateConversationBlock(updatedConv);
    };

    /**
     * Sự kiện 'conversation_unblocked':
     * Nhận thông báo khi cuộc trò chuyện 1-1 được bỏ chặn.
     * Đặt lại trường block = null trong cache để mở lại quyền gửi tin nhắn.
     */
    const onConversationUnblocked = (updatedConv: Conversation) => {
        chatCache.updateConversationBlock({ ...updatedConv, block: null });
    };

    /**
     * Sự kiện 'group_disbanded':
     * Nhận thông báo khi Trưởng nhóm quyết định giải tán vĩnh viễn nhóm chat.
     */
    const onGroupDisbanded = (data: {
        conversationId: string;
        disbanded_by: string;
        group_name?: string;
    }) => {
        // Xóa nhóm khỏi cache TanStack Query
        chatCache.removeConversation(data.conversationId);

        const { activeConversation, setActiveConversation } =
            useChatStore.getState();
        // Nếu người dùng đang mở đúng nhóm vừa bị giải tán
        if (
            activeConversation &&
            activeConversation.id === data.conversationId
        ) {
            setActiveConversation(null);
            const basePath =
                (typeof window !== "undefined" &&
                    window.location.pathname) ||
                "/chat";
            router.replace(`${basePath}`);
        }
    };

    /**
     * Sự kiện 'conversation_updated':
     * Nhận thông báo khi thông tin chung của cuộc trò chuyện được cập nhật (đổi tên nhóm, thay avatar...).
     */
    const onConversationUpdated = (updatedConv: Conversation) => {
        chatCache.updateConversationInfo(updatedConv);
    };

    // Đăng ký toàn bộ các sự kiện liên quan đến cuộc trò chuyện
    socket.on("new_conversation", onNewConversation);
    socket.on("members_added", onMembersAdded);
    socket.on("members_kicked", onMembersKicked);
    socket.on("admins_updated", onAdminsUpdated);
    socket.on("member_left", onMemberLeft);
    socket.on("conversation_blocked", onConversationBlocked);
    socket.on("conversation_unblocked", onConversationUnblocked);
    socket.on("group_disbanded", onGroupDisbanded);
    socket.on("conversation_updated", onConversationUpdated);

    // Dọn dẹp listeners khi unmount
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
