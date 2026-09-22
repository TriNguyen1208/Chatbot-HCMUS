import { Socket } from "socket.io-client";
import { useUserStore } from "../../stores/userStore";

/**
 * Đăng ký các bộ xử lý sự kiện Socket liên quan đến trạng thái hoạt động (Online / Offline) của người dùng.
 *
 * @param socket - Đối tượng kết nối Socket.IO client
 * @returns Hàm cleanup hủy lắng nghe các sự kiện socket khi component unmount
 */
export const registerPresenceHandlers = (socket: Socket) => {
    /**
     * Sự kiện 'user_online':
     * Được server phát ra khi một người dùng kết nối socket thành công.
     * Cập nhật trạng thái người dùng sang online (is_online = true) trong userStore.
     */
    const onUserOnline = (data: { userId: string }) => {
        useUserStore.getState().updateUserPresence(data.userId, true);
    };

    /**
     * Sự kiện 'user_offline':
     * Được server phát ra khi một người dùng ngắt kết nối socket.
     * Cập nhật trạng thái người dùng sang offline (is_online = false)
     * kèm theo mốc thời gian hoạt động cuối cùng (last_active).
     */
    const onUserOffline = (data: { userId: string; last_active: string }) => {
        useUserStore
            .getState()
            .updateUserPresence(data.userId, false, data.last_active);
    };

    // Đăng ký lắng nghe sự kiện từ socket
    socket.on("user_online", onUserOnline);
    socket.on("user_offline", onUserOffline);

    // Trả về hàm hủy đăng ký để tránh memory leak và duplicate listener
    return () => {
        socket.off("user_online", onUserOnline);
        socket.off("user_offline", onUserOffline);
    };
};
