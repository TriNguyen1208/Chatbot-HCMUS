import { QueryClient } from "@tanstack/react-query";
import { queryClient as defaultQueryClient } from "@/providers/QueryProvider";
import type { Message, Conversation, Watermark } from "@/types";
import { useChatStore } from "../stores/chatStore";
import { useUserStore } from "../stores/userStore";
import { conversationApi } from "../api/conversation.api";

const directIndex = new Map<string, string>(); // receiverId -> conversationId
let cachedSelfConversationId: string | null = null;

/**
 * Hàm tiện ích xử lý linh hoạt tham số truyền vào:
 * Cho phép gọi hàm theo 2 kiểu:
 * 1. chatCache.method(queryClient, ...args) - truyền queryClient tùy chỉnh (dành cho SSR hoặc testing)
 * 2. chatCache.method(...args) - tự động dùng queryClient mặc định
 */
function resolveArgs(args: any[]): { client: QueryClient; actualArgs: any[] } {
    // Nếu tham số đầu tiên là một instance của QueryClient (có chứa hàm getQueryData)
    if (args[0] && typeof args[0].getQueryData === "function") {
        return { client: args[0] as QueryClient, actualArgs: args.slice(1) };
    }
    // Nếu tham số đầu tiên bị undefined nhưng có các tham số phía sau
    if (args[0] === undefined && args.length > 1) {
        return { client: defaultQueryClient, actualArgs: args.slice(1) };
    }
    // Ngược lại, sử dụng queryClient mặc định và giữ nguyên toàn bộ tham số
    return { client: defaultQueryClient, actualArgs: args };
}

export const chatCache = {
    /**
     * Lập chỉ mục (Index) toàn bộ danh sách cuộc trò chuyện vào bộ nhớ RAM và tạo cache đơn lẻ O(1).
     * @param conversations Mảng danh sách cuộc trò chuyện vừa tải về
     * @param currentUserId ID người dùng hiện tại đang đăng nhập
     * @param client Instance QueryClient (mặc định là defaultQueryClient)
     */
    indexConversations: (
        conversations: Conversation[],
        currentUserId?: string,
        client: QueryClient = defaultQueryClient,
    ) => {
        for (const conv of conversations) {
            if (!conv?.id) continue;

            client.setQueryData(["conversation", conv.id], conv);

            if (conv.type === "self") {
                cachedSelfConversationId = conv.id;
            }

            if (conv.type === "utu" && Array.isArray(conv.member_ids)) {
                const otherId = conv.member_ids.find(
                    (m) => m !== currentUserId,
                );
                if (otherId) {
                    directIndex.set(otherId, conv.id);
                }
            }
        }
    },

    /**
     * Tìm kiếm thông tin cuộc trò chuyện theo ID với tốc độ O(1).
     * @param conversationId ID của cuộc trò chuyện cần tìm
     * @param client Instance QueryClient
     * @returns Thông tin cuộc trò chuyện hoặc undefined nếu không có
     */
    getConversation: (
        conversationId: string,
        client: QueryClient = defaultQueryClient,
    ): Conversation | undefined => {
        if (!conversationId) return undefined;

        const direct = client.getQueryData<Conversation>([
            "conversation",
            conversationId,
        ]);
        if (direct) return direct;

        const allCaches = client.getQueriesData<{ pages: Conversation[][] }>({
            queryKey: ["conversations"],
        });
        for (const [_, data] of allCaches) {
            if (data?.pages) {
                for (const page of data.pages) {
                    const match = page.find((c) => c.id === conversationId);
                    if (match) {
                        client.setQueryData(
                            ["conversation", conversationId],
                            match,
                        );
                        return match;
                    }
                }
            }
        }
        return undefined;
    },

    /**
     * Cập nhật hoặc lưu mới một cuộc trò chuyện vào cache đơn lẻ và cập nhật bảng băm RAM.
     * @param conversation Dữ liệu cuộc trò chuyện cần lưu
     * @param client Instance QueryClient
     */
    setConversation: (
        conversation: Conversation,
        client: QueryClient = defaultQueryClient,
    ): void => {
        if (!conversation?.id) return;
        client.setQueryData(["conversation", conversation.id], conversation);

        if (conversation.type === "self") {
            cachedSelfConversationId = conversation.id;
        }

        if (
            conversation.type === "utu" &&
            Array.isArray(conversation.member_ids)
        ) {
            const currentUserId =
                useChatStore.getState().activeConversation?.receiver_id;
            const otherId = conversation.member_ids.find(
                (m) => m !== currentUserId,
            );
            if (otherId) directIndex.set(otherId, conversation.id);
        }
    },

    /**
     * Tìm nhanh ID cuộc trò chuyện 1-1 dựa trên ID của người bạn (receiverId) với tốc độ O(1).
     * @param receiverId ID của người bạn chat
     * @param currentUserId ID người dùng hiện tại
     * @param client Instance QueryClient
     */
    getDirectConversationId: (
        receiverId: string,
        currentUserId?: string,
        client: QueryClient = defaultQueryClient,
    ): string | undefined => {
        if (!receiverId) return undefined;

        const indexed = directIndex.get(receiverId);
        if (indexed) return indexed;

        const allCaches = client.getQueriesData<{ pages: Conversation[][] }>({
            queryKey: ["conversations"],
        });
        for (const [_, data] of allCaches) {
            if (data?.pages) {
                for (const page of data.pages) {
                    for (const c of page) {
                        if (
                            c.id &&
                            c.type === "utu" &&
                            c.member_ids?.includes(receiverId)
                        ) {
                            if (
                                !currentUserId ||
                                c.member_ids.includes(currentUserId)
                            ) {
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
     * Tìm nhanh ID của cuộc trò chuyện "Cloud của tôi" (tự chat với mình).
     */
    getSelfConversationId: (
        currentUserId?: string,
        client: QueryClient = defaultQueryClient,
    ): string | undefined => {
        if (cachedSelfConversationId) return cachedSelfConversationId;

        const allCaches = client.getQueriesData<{ pages: Conversation[][] }>({
            queryKey: ["conversations"],
        });
        for (const [_, data] of allCaches) {
            if (data?.pages) {
                for (const page of data.pages) {
                    for (const c of page) {
                        if (c.id && c.type === "self") {
                            if (
                                !currentUserId ||
                                c.member_ids?.includes(currentUserId)
                            ) {
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

    /**
     * Đánh dấu cache danh sách cuộc trò chuyện đã cũ (stale) để React Query tự động refetch lại từ máy chủ.
     */
    invalidateConversations: (client: QueryClient = defaultQueryClient) => {
        client.invalidateQueries({ queryKey: ["conversations"] });
    },

    /**
     * Thêm một tin nhắn mới vào đầu cache danh sách tin nhắn ['messages', convId] (Optimistic Update hoặc khi nhận socket).
     * Bao gồm cơ chế khử trùng lặp (Dedup) để đảm bảo không bị nhân bản tin nhắn khi nhận lại từ socket.
     */
    appendNewMessage: (...args: any[]) => {
        const { client, actualArgs } = resolveArgs(args);
        const message: Message = actualArgs[0];
        if (!message || !message.conversation_id) return;

        client.setQueryData(
            ["messages", message.conversation_id],
            (
                oldData: { pages: Message[][]; pageParams: any[] } | undefined,
            ) => {
                if (!oldData) {
                    return {
                        pages: [[message]],
                        pageParams: [undefined],
                    };
                }

                const msgId = message.id;

                const newPages = oldData.pages.map((page: Message[]) =>
                    page.filter((m: Message) => {
                        const id = m.id;
                        return id !== msgId;
                    }),
                );

                newPages[0] = [message, ...(newPages[0] || [])];

                return { ...oldData, pages: newPages };
            },
        );
    },

    /**
     * Đẩy cuộc trò chuyện vừa có tin nhắn mới lên vị trí ĐẦU TIÊN của danh sách chat ở Sidebar (Bump).
     * Cập nhật last_message của cuộc trò chuyện đó.
     */
    bumpConversationLastMessage: (...args: any[]) => {
        const { client, actualArgs } = resolveArgs(args);
        const message: Message = actualArgs[0];
        if (!message || !message.conversation_id) return;

        let foundInAnyCache = false;

        // Cập nhật tất cả các query có tiền tố ['conversations'] (bao gồm cả tab all, utu, group)
        client.setQueriesData(
            { queryKey: ["conversations"] },
            (
                oldData:
                    { pages: Conversation[][]; pageParams: any[] } | undefined,
            ) => {
                if (!oldData) return oldData;

                let updatedConv: Conversation | null = null;
                // Khử trùng lặp: Lọc cuộc trò chuyện này ra khỏi các trang cũ
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

                // Nếu tìm thấy cuộc trò chuyện này trong cache
                if (updatedConv) {
                    foundInAnyCache = true;
                    newPages[0] = [updatedConv, ...(newPages[0] || [])];
                    client.setQueryData(
                        ["conversation", (updatedConv as Conversation).id],
                        updatedConv,
                    );
                }

                return { ...oldData, pages: newPages };
            },
        );

        if (!foundInAnyCache) {
            // 1. Kiểm tra xem cuộc trò chuyện này đã có trong cache đơn lẻ ['conversation', id] chưa
            const cached = client.getQueryData<Conversation>([
                "conversation",
                message.conversation_id,
            ]);
            if (cached) {
                const updated = {
                    ...cached,
                    last_message: message,
                };
                // Thêm vào đầu danh sách chat
                chatCache.addNewConversation(updated, client);
            } else {
                // 2. Tải thông tin đúng 1 cuộc trò chuyện này từ server và chèn vào đầu (không cần refetch toàn bộ trang)
                conversationApi
                    .getConversationById(message.conversation_id)
                    .then((conv) => {
                        if (conv) {
                            const updated = {
                                ...conv,
                                last_message: message,
                            };
                            chatCache.addNewConversation(updated, client);
                        }
                    })
                    .catch((err) => {
                        console.error(
                            "Failed to fetch single conversation for cache bump:",
                            err,
                        );
                    });
            }
        }
    },

    /**
     * Cập nhật nội dung của một tin nhắn khi người dùng chỉnh sửa tin nhắn (edit message).
     * Đồng thời cập nhật last_message ở Sidebar nếu tin nhắn được sửa là tin nhắn cuối cùng.
     */
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

        // 1. Cập nhật trong khung chat danh sách tin nhắn ['messages', convId]
        client.setQueryData(
            ["messages", data.conversation_id],
            (
                oldData: { pages: Message[][]; pageParams: any[] } | undefined,
            ) => {
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
                            : msg,
                    ),
                );
                return { ...oldData, pages: newPages };
            },
        );

        // 2. Cập nhật last_message ở Sidebar nếu tin nhắn vừa sửa chính là tin nhắn cuối cùng
        client.setQueriesData(
            { queryKey: ["conversations"] },
            (
                oldData:
                    { pages: Conversation[][]; pageParams: any[] } | undefined,
            ) => {
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
                            client.setQueryData(
                                ["conversation", conv.id],
                                updated,
                            );
                            return updated;
                        }
                        return conv;
                    }),
                );
                return { ...oldData, pages: newPages };
            },
        );
    },

    /**
     * Cập nhật danh sách cảm xúc (Reactions) của tin nhắn trong khung chat và đồng bộ last_message nếu cần.
     */
    updateMessageReaction: (...args: any[]) => {
        const { client, actualArgs } = resolveArgs(args);
        const data: {
            conversation_id: string;
            message_id: string;
            reactions: any[];
        } = actualArgs[0];
        if (!data?.conversation_id) return;

        // 1. Cập nhật mảng reactions cho tin nhắn tương ứng trong khung chat
        client.setQueryData(
            ["messages", data.conversation_id],
            (
                oldData: { pages: Message[][]; pageParams: any[] } | undefined,
            ) => {
                if (!oldData) return oldData;
                const newPages = oldData.pages.map((page: Message[]) =>
                    page.map((msg: Message) =>
                        msg.id === data.message_id
                            ? { ...msg, reactions: data.reactions }
                            : msg,
                    ),
                );
                return { ...oldData, pages: newPages };
            },
        );

        // 2. Cập nhật reactions trong last_message ở Sidebar nếu đây là tin nhắn cuối
        client.setQueriesData(
            { queryKey: ["conversations"] },
            (
                oldData:
                    { pages: Conversation[][]; pageParams: any[] } | undefined,
            ) => {
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
                                last_message: {
                                    ...conv.last_message,
                                    reactions: data.reactions,
                                },
                            };
                            client.setQueryData(
                                ["conversation", conv.id],
                                updated,
                            );
                            return updated;
                        }
                        return conv;
                    }),
                );
                return { ...oldData, pages: newPages };
            },
        );
    },

    /**
     * Đánh dấu một tin nhắn là ĐÃ BỊ THU HỒI (status: 'recalled') trong khung chat và Sidebar.
     */
    markMessageRecalled: (...args: any[]) => {
        const { client, actualArgs } = resolveArgs(args);
        const data: { conversation_id: string; messageId: string } =
            actualArgs[0];
        if (!data?.conversation_id) return;

        // 1. Cập nhật trạng thái 'recalled' cho tin nhắn trong khung chat
        client.setQueryData(
            ["messages", data.conversation_id],
            (
                oldData: { pages: Message[][]; pageParams: any[] } | undefined,
            ) => {
                if (!oldData) return oldData;
                const newPages = oldData.pages.map((page: Message[]) =>
                    page.map((msg: Message) =>
                        msg.id === data.messageId
                            ? { ...msg, status: "recalled" }
                            : msg,
                    ),
                );
                return { ...oldData, pages: newPages };
            },
        );

        // 2. Cập nhật status của last_message ở Sidebar
        client.setQueriesData(
            { queryKey: ["conversations"] },
            (
                oldData:
                    { pages: Conversation[][]; pageParams: any[] } | undefined,
            ) => {
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
                                    status: "recalled" as const,
                                },
                            };
                            client.setQueryData(
                                ["conversation", conv.id],
                                updated,
                            );
                            return updated;
                        }
                        return conv;
                    }),
                );
                return { ...oldData, pages: newPages };
            },
        );
    },

    /**
     * Thêm một cuộc trò chuyện mới tạo (Nhóm hoặc 1-1) lên ĐẦU danh sách cache của Sidebar.
     * Cập nhật đồng thời cho cả tab Tổng hợp ('conversations') và tab lọc theo loại ('conversations', type).
     */
    addNewConversation: (...args: any[]) => {
        const { client, actualArgs } = resolveArgs(args);
        const conversation: Conversation = actualArgs[0];
        if (!conversation?.id) return;

        // 1. Lưu vào cache đơn lẻ ['conversation', id] để tra cứu O(1)
        client.setQueryData(["conversation", conversation.id], conversation);

        if (conversation.type === "self") {
            cachedSelfConversationId = conversation.id;
        }

        // Hàm helper cập nhật chèn vào đầu trang 0 cho 1 queryKey cụ thể
        const updateCache = (queryKey: string[]) => {
            client.setQueryData(
                queryKey,
                (
                    oldData:
                        | { pages: Conversation[][]; pageParams: any[] }
                        | undefined,
                ) => {
                    // Nếu cache chưa có gì -> tạo mới mảng trang đầu tiên
                    if (!oldData) {
                        return {
                            pages: [[conversation]],
                            pageParams: [undefined],
                        };
                    }
                    // Khử trùng lặp: lọc bỏ cuộc trò chuyện này nếu nó đã tồn tại ở bất kỳ trang nào
                    const newPages = oldData.pages.map((page: Conversation[]) =>
                        page.filter(
                            (c: Conversation) => c.id !== conversation.id,
                        ),
                    );
                    // Chèn lên đầu trang 0
                    newPages[0] = [conversation, ...(newPages[0] || [])];
                    return { ...oldData, pages: newPages };
                },
            );
        };

        // Cập nhật tab "Tất cả" (All Chat)
        updateCache(["conversations"]);
        // Nếu có loại (utu hoặc group) -> Cập nhật tiếp cho tab tương ứng (Direct Chat hoặc Group Chat)
        if (conversation.type) {
            updateCache(["conversations", conversation.type]);
        }
    },

    /**
     * Cập nhật mốc đọc tin nhắn / nhận tin nhắn (Watermark - Delivered / Read) của các thành viên.
     *
     * Khái niệm "Watermark":
     * - Thay vì lưu trạng thái đã đọc/nhận trên từng tin nhắn riêng lẻ (tốn tài nguyên DB),
     *   hệ thống lưu một "mốc mực nước" (Watermark) gắn với từng User:
     *   + last_delivered_msg_id: ID của tin nhắn mới nhất mà thiết bị user đó ĐÃ NHẬN ĐƯỢC (dấu tick đôi xám).
     *   + last_read_msg_id: ID của tin nhắn mới nhất mà user đó ĐÃ XEM / ĐÃ ĐỌC (dấu tick xanh hoặc avatar nhỏ).
     * - Mọi tin nhắn nằm trước mốc này mặc nhiên được coi là đã nhận / đã xem.
     *
     * Hàm này đồng bộ mốc watermark mới vào 3 nơi:
     * 1. Cache danh sách cuộc trò chuyện ở Sidebar (QueryKey: ["conversations"])
     * 2. Cache chi tiết của cuộc hội thoại (QueryKey: ["conversation", id])
     * 3. State đang mở trên màn hình (Zustand: activeConversation)
     */
    updateConversationWatermarks: (...args: any[]) => {
        const { client, actualArgs } = resolveArgs(args);
        const data: {
            conversationId: string;
            userId: string;
            messageId: string;
            type: "delivered" | "read";
        } = actualArgs[0];
        if (!data?.conversationId) return;

        /**
         * Hàm helper: Hợp nhất (merge) mảng watermarks hiện có với sự kiện watermark mới từ Socket.
         * Sử dụng cấu trúc Map<userId, Watermark> để:
         * - Khử trùng lặp: Đảm bảo mỗi user chỉ có đúng 1 bản ghi watermark.
         * - Cập nhật nguyên tử: Giữ nguyên các mốc cũ nếu sự kiện mới không đè lên.
         */
        const mergeWatermarks = (
            currentWatermarks: Watermark[] = [],
        ): Watermark[] => {
            const map = new Map<string, Watermark>();

            // Bước 1: Duyệt qua mảng watermarks hiện tại để nạp vào Map theo key là userId
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
                    // Nếu user đã tồn tại trong Map -> giữ lại mốc có giá trị hợp lệ nhất
                    map.set(uid, {
                        user_id: uid,
                        last_delivered_msg_id:
                            w.last_delivered_msg_id ||
                            prev.last_delivered_msg_id ||
                            null,
                        last_read_msg_id:
                            w.last_read_msg_id || prev.last_read_msg_id || null,
                    });
                }
            }

            // Bước 2: Lấy ra hoặc khởi tạo watermark cho user vừa phát sinh sự kiện
            const targetUid = String(data.userId);
            const existing = map.get(targetUid) || {
                user_id: targetUid,
                last_delivered_msg_id: null,
                last_read_msg_id: null,
            };

            // Bước 3: Áp dụng mốc mới tùy theo loại sự kiện
            if (data.type === "delivered") {
                // Sự kiện 'delivered': Thiết bị đối phương vừa nhận được tin nhắn
                existing.last_delivered_msg_id = data.messageId;
            } else if (data.type === "read") {
                // Sự kiện 'read': Đối phương đã mở xem tin nhắn
                // Quy tắc nghiệp vụ: Đã đọc tin nhắn thì chắc chắn đã nhận tin nhắn đó,
                // do đó cập nhật đồng thời cả last_read_msg_id và last_delivered_msg_id
                existing.last_read_msg_id = data.messageId;
                existing.last_delivered_msg_id = data.messageId;
            }
            map.set(targetUid, existing);

            // Chuyển Map trở lại thành mảng Watermark[]
            return Array.from(map.values());
        };

        // BƯỚC 1: Cập nhật cache TanStack Query cho danh sách các cuộc trò chuyện ở Sidebar.
        // Dùng setQueriesData ({ queryKey: ["conversations"] }) để tự động áp dụng cho TẤT CẢ các tab:
        // tab "Tất cả" (["conversations"]), tab "1-1" (["conversations", "utu"]), tab "Nhóm" (["conversations", "group"])
        client.setQueriesData(
            { queryKey: ["conversations"] },
            (
                oldData:
                    { pages: Conversation[][]; pageParams: any[] } | undefined,
            ) => {
                if (!oldData) return oldData;
                const newPages = oldData.pages.map((page: Conversation[]) =>
                    page.map((conv: Conversation) => {
                        if (conv.id === data.conversationId) {
                            const newWatermarks = mergeWatermarks(
                                conv.watermarks,
                            );
                            const updated = {
                                ...conv,
                                watermarks: newWatermarks,
                            };
                            // BƯỚC 2: Đồng thời cập nhật luôn vào cache đơn lẻ ['conversation', id]
                            client.setQueryData(
                                ["conversation", conv.id],
                                updated,
                            );
                            return updated;
                        }
                        return conv;
                    }),
                );
                return { ...oldData, pages: newPages };
            },
        );

        // BƯỚC 3: Đồng bộ vào activeConversation của Zustand store (nếu người dùng đang mở đúng cuộc trò chuyện này)
        // Điều này giúp khung chat hiện tại lập tức hiển thị avatar/tick đã đọc mà không cần đợi refetch
        const { activeConversation, setActiveConversation } =
            useChatStore.getState();
        if (
            activeConversation &&
            activeConversation.id === data.conversationId
        ) {
            const newWatermarks = mergeWatermarks(
                activeConversation.watermarks,
            );
            setActiveConversation({
                ...activeConversation,
                watermarks: newWatermarks,
            });
        }
    },

    /**
     * Cập nhật trạng thái Chặn (Block) hoặc Bỏ chặn (Unblock) cuộc trò chuyện trong cache và Zustand store.
     */
    updateConversationBlock: (...args: any[]) => {
        const { client, actualArgs } = resolveArgs(args);
        const updatedConv: Conversation = actualArgs[0];
        if (!updatedConv?.id) return;

        // 1. Cập nhật cache đơn lẻ ['conversation', id]
        client.setQueryData(["conversation", updatedConv.id], updatedConv);

        // 2. Cập nhật trong danh sách cache ['conversations']
        client.setQueriesData(
            { queryKey: ["conversations"] },
            (
                oldData:
                    { pages: Conversation[][]; pageParams: any[] } | undefined,
            ) => {
                if (!oldData) return oldData;
                const newPages = oldData.pages.map((page: Conversation[]) =>
                    page.map((conv: Conversation) =>
                        conv.id === updatedConv.id
                            ? { ...conv, ...updatedConv }
                            : conv,
                    ),
                );
                return { ...oldData, pages: newPages };
            },
        );

        // 3. Đồng bộ vào Zustand store nếu là cuộc trò chuyện đang active
        const { activeConversation, setActiveConversation } =
            useChatStore.getState();
        if (activeConversation && activeConversation.id === updatedConv.id) {
            setActiveConversation({ ...activeConversation, ...updatedConv });
        }
    },

    /**
     * Xóa một cuộc trò chuyện ra khỏi cache (khi bị kick, rời nhóm hoặc giải tán nhóm).
     */
    removeConversation: (...args: any[]) => {
        const { client, actualArgs } = resolveArgs(args);
        const conversationId: string = actualArgs[0];
        if (!conversationId) return;

        // 1. Xóa bỏ hoàn toàn query đơn lẻ của cuộc trò chuyện này
        client.removeQueries({ queryKey: ["conversation", conversationId] });

        // 2. Lọc bỏ cuộc trò chuyện này khỏi tất cả các trang trong cache danh sách ['conversations']
        client.setQueriesData(
            { queryKey: ["conversations"] },
            (
                oldData:
                    { pages: Conversation[][]; pageParams: any[] } | undefined,
            ) => {
                if (!oldData) return oldData;
                const newPages = oldData.pages.map((page: Conversation[]) =>
                    page.filter(
                        (conv: Conversation) => conv.id !== conversationId,
                    ),
                );
                return { ...oldData, pages: newPages };
            },
        );
    },

    /**
     * Cập nhật thông tin cuộc trò chuyện (đổi tên nhóm, đổi ảnh đại diện nhóm).
     */
    updateConversationInfo: (...args: any[]) => {
        const { client, actualArgs } = resolveArgs(args);
        const updatedConv: Conversation = actualArgs[0];
        if (!updatedConv?.id) return;

        // 1. Cập nhật cache đơn lẻ ['conversation', id]
        client.setQueryData(["conversation", updatedConv.id], updatedConv);

        // 2. Cập nhật trong danh sách phân trang ['conversations']
        client.setQueriesData(
            { queryKey: ["conversations"] },
            (
                oldData:
                    { pages: Conversation[][]; pageParams: any[] } | undefined,
            ) => {
                if (!oldData) return oldData;
                const newPages = oldData.pages.map((page: Conversation[]) =>
                    page.map((conv: Conversation) =>
                        conv.id === updatedConv.id
                            ? { ...conv, ...updatedConv }
                            : conv,
                    ),
                );
                return { ...oldData, pages: newPages };
            },
        );

        // 3. Đồng bộ với Header khung chat hiện tại nếu đang mở đúng cuộc trò chuyện này
        const { activeConversation, setActiveConversation } =
            useChatStore.getState();
        if (activeConversation && activeConversation.id === updatedConv.id) {
            setActiveConversation({ ...activeConversation, ...updatedConv });
        }
    },

    /**
     * Thêm danh sách thành viên mới vào cuộc trò chuyện.
     * Tự động loại bỏ ID trùng lặp (Set) và kích hoạt requestUser để tải trước profile thành viên mới.
     */
    addMembersToConversation: (...args: any[]) => {
        const { client, actualArgs } = resolveArgs(args);
        const conversationId: string = actualArgs[0];
        const newMemberIds: string[] = actualArgs[1];
        if (!conversationId) return;

        // Cập nhật member_ids trong cache danh sách cuộc trò chuyện
        client.setQueriesData(
            { queryKey: ["conversations"] },
            (
                oldData:
                    { pages: Conversation[][]; pageParams: any[] } | undefined,
            ) => {
                if (!oldData) return oldData;
                const newPages = oldData.pages.map((page: Conversation[]) =>
                    page.map((conv: Conversation) => {
                        if (conv.id === conversationId) {
                            const currentMembers = conv.member_ids || [];
                            // Hợp nhất danh sách thành viên cũ và mới, loại bỏ trùng lặp bằng Set
                            const combined = Array.from(
                                new Set([...currentMembers, ...newMemberIds]),
                            );
                            const updated = { ...conv, member_ids: combined };
                            client.setQueryData(
                                ["conversation", conv.id],
                                updated,
                            );
                            return updated;
                        }
                        return conv;
                    }),
                );
                return { ...oldData, pages: newPages };
            },
        );

        // Đồng bộ vào cuộc trò chuyện đang active nếu trùng ID
        const { activeConversation, setActiveConversation } =
            useChatStore.getState();
        if (activeConversation && activeConversation.id === conversationId) {
            const currentMembers = activeConversation.member_ids || [];
            const combined = Array.from(
                new Set([...currentMembers, ...newMemberIds]),
            );
            setActiveConversation({
                ...activeConversation,
                member_ids: combined,
            });
        }

        // Kích hoạt requestUser để fetch trước thông tin (tên, avatar) của các thành viên mới vào RAM
        newMemberIds?.forEach((id) => {
            useUserStore.getState().requestUser(id);
        });
    },

    /**
     * Xóa các thành viên bị kick hoặc tự rời khỏi cuộc trò chuyện.
     * Cập nhật đồng thời danh sách member_ids và admin_ids.
     */
    removeMembersFromConversation: (...args: any[]) => {
        const { client, actualArgs } = resolveArgs(args);
        const conversationId: string = actualArgs[0];
        const removedMemberIds: string[] = actualArgs[1];
        if (!conversationId) return;

        client.setQueriesData(
            { queryKey: ["conversations"] },
            (
                oldData:
                    { pages: Conversation[][]; pageParams: any[] } | undefined,
            ) => {
                if (!oldData) return oldData;
                const newPages = oldData.pages.map((page: Conversation[]) =>
                    page.map((conv: Conversation) => {
                        if (conv.id === conversationId) {
                            // Lọc bỏ các ID thành viên bị xóa
                            const updatedMembers = (
                                conv.member_ids || []
                            ).filter((id) => !removedMemberIds.includes(id));
                            // Lọc bỏ luôn trong danh sách admin nếu thành viên đó từng là admin
                            const updatedAdmins = (conv.admin_ids || []).filter(
                                (id) => !removedMemberIds.includes(id),
                            );
                            const updated = {
                                ...conv,
                                member_ids: updatedMembers,
                                admin_ids: updatedAdmins,
                            };
                            client.setQueryData(
                                ["conversation", conv.id],
                                updated,
                            );
                            return updated;
                        }
                        return conv;
                    }),
                );
                return { ...oldData, pages: newPages };
            },
        );

        // Đồng bộ vào activeConversation trong Zustand store
        const { activeConversation, setActiveConversation } =
            useChatStore.getState();
        if (activeConversation && activeConversation.id === conversationId) {
            const updatedMembers = (activeConversation.member_ids || []).filter(
                (id) => !removedMemberIds.includes(id),
            );
            const updatedAdmins = (activeConversation.admin_ids || []).filter(
                (id) => !removedMemberIds.includes(id),
            );
            setActiveConversation({
                ...activeConversation,
                member_ids: updatedMembers,
                admin_ids: updatedAdmins,
            });
        }
    },

    /**
     * Cập nhật danh sách Quản trị viên (Admins) trong cuộc trò chuyện nhóm.
     */
    updateAdminsInConversation: (...args: any[]) => {
        const { client, actualArgs } = resolveArgs(args);
        const conversationId: string = actualArgs[0];
        const newAdminIds: string[] = actualArgs[1];
        if (!conversationId) return;

        client.setQueriesData(
            { queryKey: ["conversations"] },
            (
                oldData:
                    { pages: Conversation[][]; pageParams: any[] } | undefined,
            ) => {
                if (!oldData) return oldData;
                const newPages = oldData.pages.map((page: Conversation[]) =>
                    page.map((conv: Conversation) => {
                        if (conv.id === conversationId) {
                            const currentAdmins = conv.admin_ids || [];
                            // Hợp nhất danh sách admin mới và cũ, loại bỏ trùng lặp
                            const combined = Array.from(
                                new Set([...currentAdmins, ...newAdminIds]),
                            );
                            const updated = { ...conv, admin_ids: combined };
                            client.setQueryData(
                                ["conversation", conv.id],
                                updated,
                            );
                            return updated;
                        }
                        return conv;
                    }),
                );
                return { ...oldData, pages: newPages };
            },
        );

        // Đồng bộ vào activeConversation
        const { activeConversation, setActiveConversation } =
            useChatStore.getState();
        if (activeConversation && activeConversation.id === conversationId) {
            const currentAdmins = activeConversation.admin_ids || [];
            const combined = Array.from(
                new Set([...currentAdmins, ...newAdminIds]),
            );
            setActiveConversation({
                ...activeConversation,
                admin_ids: combined,
            });
        }
    },
};
