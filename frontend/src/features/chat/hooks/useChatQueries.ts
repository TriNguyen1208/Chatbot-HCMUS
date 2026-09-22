import { useInfiniteQuery } from "@tanstack/react-query";
import { conversationApi } from "../api/conversation.api";
import { messageApi } from "../api/message.api";
import { Conversation, Message } from "../types";
import { useUserStore } from "../stores/userStore";

export const useConversationsQuery = (type?: "utu" | "group") => {
    return useInfiniteQuery({
        queryKey: type ? ["conversations", type] : ["conversations"],

        queryFn: async ({ pageParam }) => {
            const res = await conversationApi.getConversations(
                20,
                pageParam as string | undefined,
                type,
            );
            const conversations = res;

            const userStoreState = useUserStore.getState();
            const existingUsers = userStoreState.users;

            // Duyệt qua từng cuộc trò chuyện vừa tải về để fetch trước (prefetch) thông tin user bị thiếu
            conversations.forEach((conv) => {
                conv.member_ids?.forEach((memberId) => {
                    if (!existingUsers[memberId]) {
                        userStoreState.requestUser(memberId);
                    }
                });

                if (
                    conv.last_message?.sender_id &&
                    !existingUsers[conv.last_message.sender_id]
                ) {
                    userStoreState.requestUser(conv.last_message.sender_id);
                }
            });

            // Trả về mảng danh sách cuộc trò chuyện cho React Query lưu vào cache của trang hiện tại
            return conversations;
        },

        // Giá trị cursor khởi tạo cho trang đầu tiên (lần fetch đầu không cần cursor nên để undefined)
        initialPageParam: undefined as string | undefined,

        // Hàm xác định con trỏ (cursor) cho trang tiếp theo
        // lastPage: dữ liệu mảng các cuộc trò chuyện vừa nhận được ở trang gần nhất
        getNextPageParam: (lastPage: Conversation[]) => {
            // Nếu trang vừa rồi lấy đủ 20 item -> khả năng cao vẫn còn trang tiếp theo
            if (lastPage && lastPage.length === 20) {
                // Lấy phần tử cuối cùng của trang
                const lastItem = lastPage[lastPage.length - 1];
                // Trả về ID tin nhắn cuối (hoặc ID conversation) để làm con trỏ cursor_id cho lần gọi kế tiếp
                return lastItem.last_message?.id || lastItem.id;
            }
            // Nếu ít hơn 20 item -> đã hết dữ liệu, không fetch thêm nữa (hasNextPage = false)
            return undefined;
        },
    });
};

/**
 * Hook tải danh sách tin nhắn của 1 cuộc trò chuyện cụ thể (cuộn vô tận ngược lên trên để xem tin cũ)
 * @param conversationId ID của cuộc trò chuyện cần lấy tin nhắn
 */
export const useMessagesQuery = (conversationId?: string) => {
    return useInfiniteQuery({
        // Khóa định danh cache theo từng conversationId riêng biệt
        queryKey: ["messages", conversationId],

        // Hàm gọi API lấy tin nhắn của từng trang
        queryFn: async ({ pageParam }) => {
            // Nếu chưa chọn cuộc trò chuyện nào thì trả về mảng rỗng ngay lập tức
            if (!conversationId) return [];

            // Gọi API lấy 20 tin nhắn tiếp theo tính từ cursor mốc (pageParam)
            const res = await messageApi.getMessages(
                conversationId,
                20,
                pageParam as string | undefined,
            );
            return res;
        },

        // Mốc cursor khởi tạo cho trang đầu tiên (mặc định undefined để lấy các tin nhắn mới nhất)
        initialPageParam: undefined as string | undefined,

        // Hàm xác định con trỏ cursor cho trang tin nhắn cũ hơn
        getNextPageParam: (lastPage: Message[]) => {
            // Nếu trang vừa rồi trả về đủ 20 tin nhắn -> còn tin nhắn cũ hơn nữa
            if (lastPage && lastPage.length === 20) {
                // Lấy tin nhắn cũ nhất (nằm ở cuối mảng) để làm mốc cursorId lấy tiếp các tin cũ hơn
                const lastItem = lastPage[lastPage.length - 1];
                return lastItem.id;
            }
            // Trả về undefined nếu đã chạm đáy/hết tin nhắn cũ
            return undefined;
        },

        // Chỉ tự động kích hoạt gọi API khi có conversationId hợp lệ (tránh gọi API khi chưa chọn box chat)
        enabled: !!conversationId,
    });
};
