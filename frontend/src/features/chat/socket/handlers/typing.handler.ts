import { Socket } from "socket.io-client";
import { useChatStore } from "../../stores/chatStore";

/**
 * Đăng ký các bộ xử lý sự kiện Socket liên quan đến trạng thái đang soạn tin (Typing indicator).
 *
 * @param socket - Đối tượng kết nối Socket.IO client
 * @returns Hàm cleanup hủy lắng nghe các sự kiện socket khi component unmount
 */
export const registerTypingHandlers = (socket: Socket) => {
    /**
     * Sự kiện 'typing':
     * Nhận thông báo khi một người dùng trong cuộc hội thoại bắt đầu gõ bàn phím.
     * Lưu thông tin người dùng vào danh sách typingUsers của conversation tương ứng trong chatStore.
     */
    const onTyping = (data: {
        conversationId: string;
        userId: string;
        name: string;
    }) => {
        useChatStore
            .getState()
            .addTypingUser(data.conversationId, data.userId, data.name);
    };

    /**
     * Sự kiện 'stop_typing':
     * Nhận thông báo khi người dùng dừng gõ phím hoặc sau một khoảng timeout không có thao tác.
     * Xóa người dùng đó khỏi danh sách typingUsers của conversation trong chatStore.
     */
    const onStopTyping = (data: { conversationId: string; userId: string }) => {
        useChatStore
            .getState()
            .removeTypingUser(data.conversationId, data.userId);
    };

    // Đăng ký lắng nghe sự kiện
    socket.on("typing", onTyping);
    socket.on("stop_typing", onStopTyping);

    // Dọn dẹp listener khi unmount
    return () => {
        socket.off("typing", onTyping);
        socket.off("stop_typing", onStopTyping);
    };
};
