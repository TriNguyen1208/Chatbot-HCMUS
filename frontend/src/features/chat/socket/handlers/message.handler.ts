import { Socket } from "socket.io-client";
import type { Message } from "@/types";
import { chatCache } from "../../utils/chat-cache.util";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useChatStore } from "../../stores/chatStore";

/**
 * Đăng ký các bộ xử lý sự kiện Socket liên quan đến tin nhắn (Message Lifecycle):
 * Nhận tin nhắn mới, chỉnh sửa, thu hồi, thả cảm xúc (reaction), và trạng thái đọc/nhận.
 */
export const registerMessageHandlers = (socket: Socket) => {
    /**
     * Sự kiện 'new_message':
     * Nhận tin nhắn mới theo thời gian thực từ server (do người khác gửi đến hoặc chính mình gửi từ thiết bị khác).
     */
    const onNewMessage = (message: Message) => {
        console.log("📨 [Socket] new_message:", message);
        const { activeConversation } = useChatStore.getState();
        const { user } = useAuthStore.getState();

        // Tự động phản hồi 'mark_read' hoặc 'mark_delivered' nếu tin nhắn đến từ người khác
        if (
            user?.id &&
            message.sender_id !== user.id &&
            message.conversation_id &&
            message.id
        ) {
            const isActive = activeConversation?.id === message.conversation_id;
            if (isActive) {
                // Nếu người dùng đang mở đúng cuộc trò chuyện này -> Báo đã đọc ngay lập tức
                socket.emit("mark_read", {
                    conversationId: message.conversation_id,
                    messageId: message.id,
                });
            } else {
                // Nếu người dùng đang ở màn hình khác hoặc mở cuộc trò chuyện khác -> Báo đã nhận (delivered)
                socket.emit("mark_delivered", {
                    conversationId: message.conversation_id,
                    messageId: message.id,
                });
            }
        }

        // 1. Nạp tin nhắn mới vào đầu cache danh sách tin nhắn của conversation này
        chatCache.appendNewMessage(message);

        // 2. Cập nhật 'last_message' và đẩy cuộc trò chuyện này lên vị trí đầu tiên của danh sách chat ở Sidebar
        chatCache.bumpConversationLastMessage(message);
    };

    /**
     * Sự kiện 'message_edited':
     * Nhận thông báo một tin nhắn đã được người gửi chỉnh sửa nội dung.
     * Cập nhật nội dung mới và lịch sử chỉnh sửa vào cache tin nhắn.
     */
    const onMessageEdited = (data: {
        conversation_id: string;
        messageId: string;
        content: string;
        updated_at: string;
        edit_history?: { content: string; updated_at: string | Date }[];
    }) => {
        chatCache.updateMessageContent(data);
    };

    /**
     * Sự kiện 'message_reaction_updated':
     * Nhận thông báo danh sách reaction (thả tim, like, haha...) của tin nhắn bị thay đổi.
     * Cập nhật mảng reactions của tin nhắn trong cache.
     */
    const onMessageReactionUpdated = (data: {
        conversation_id: string;
        message_id: string;
        reactions: any[];
    }) => {
        chatCache.updateMessageReaction(data);
    };

    /**
     * Sự kiện 'message_recalled':
     * Nhận thông báo một tin nhắn đã bị thu hồi bởi người gửi.
     * Đánh dấu is_recalled = true và xóa nội dung tin nhắn trong cache hiển thị.
     */
    const onMessageRecalled = (data: {
        conversation_id: string;
        messageId: string;
    }) => {
        chatCache.markMessageRecalled(data);
    };

    /**
     * Sự kiện 'message_save_failed':
     * Nhận thông báo từ server khi việc lưu tin nhắn vào cơ sở dữ liệu gặp lỗi.
     */
    const onMessageSaveFailed = (data: {
        conversationId: string;
        messageId: string;
        error?: string;
    }) => {
        console.error("❌ Message save failed:", data);
    };

    // Đăng ký toàn bộ các sự kiện liên quan đến tin nhắn
    socket.on("new_message", onNewMessage);
    socket.on("message_edited", onMessageEdited);
    socket.on("message_reaction_updated", onMessageReactionUpdated);
    socket.on("message_recalled", onMessageRecalled);
    socket.on("message_save_failed", onMessageSaveFailed);

    // Dọn dẹp listeners khi unmount để chống rò rỉ bộ nhớ
    return () => {
        socket.off("new_message", onNewMessage);
        socket.off("message_edited", onMessageEdited);
        socket.off("message_reaction_updated", onMessageReactionUpdated);
        socket.off("message_recalled", onMessageRecalled);
        socket.off("message_save_failed", onMessageSaveFailed);
    };
};
