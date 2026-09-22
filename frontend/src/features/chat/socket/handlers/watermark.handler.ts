import { Socket } from "socket.io-client";
import { chatCache } from "../../utils/chat-cache.util";

/**
 * Đăng ký các bộ xử lý sự kiện Socket liên quan đến Watermark tin nhắn (Trạng thái đã gửi, đã nhận, đã xem).
 *
 * @param socket - Đối tượng kết nối Socket.IO client
 * @returns Hàm cleanup hủy lắng nghe các sự kiện socket khi component unmount
 */
export const registerWatermarkHandlers = (socket: Socket) => {
    /**
     * Sự kiện 'watermark_updated':
     * Được server phát khi đối phương đã nhận (delivered) hoặc đã đọc (read) tin nhắn đến một mốc messageId nhất định.
     * Cập nhật lại watermark trong cache TanStack Query để giao diện hiển thị dấu tích đôi hoặc avatar đã xem.
     */
    const onWatermarkUpdated = (data: {
        conversationId: string;
        userId: string;
        messageId: string;
        type: "delivered" | "read";
    }) => {
        chatCache.updateConversationWatermarks(data);
    };

    // Đăng ký lắng nghe sự kiện
    socket.on("watermark_updated", onWatermarkUpdated);

    // Dọn dẹp listener khi unmount
    return () => {
        socket.off("watermark_updated", onWatermarkUpdated);
    };
};
