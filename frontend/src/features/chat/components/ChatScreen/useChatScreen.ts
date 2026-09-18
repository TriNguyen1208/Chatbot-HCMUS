import { useEffect, useRef } from "react";
import { useSearchParams, useRouter, usePathname } from "next/navigation";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { conversationApi } from "@/features/chat/api/conversation.api";
import { userApi } from "@/features/chat/api/user.api";
import { useSocketContext } from "@/providers/SocketProvider";
import { chatCache } from "@/features/chat/utils/chat-cache.util";

export const useChatScreen = (type?: "utu" | "group" | "all") => {
    const { activeConversation, setActiveConversation } = useChatStore();
    const searchParams = useSearchParams();
    const router = useRouter();
    const pathname = usePathname();
    const { socket } = useSocketContext();

    const fallbackRoute = type === "group" ? "/group-chat" : type === "utu" ? "/direct-chat" : "/chat";

    const cId = searchParams.get("conversation_id");
    const receiverId = searchParams.get("receiver_id");
    const activeConvRef = useRef(activeConversation);
    activeConvRef.current = activeConversation;

    // Emit mark_read when active conversation receives or views messages
    useEffect(() => {
        if (activeConversation?.id && activeConversation.last_message?.id && socket) {
            const currentUser = useAuthStore.getState().user;
            if (currentUser?.id && activeConversation.last_message.sender_id !== currentUser.id) {
                socket.emit("mark_read", {
                    conversationId: activeConversation.id,
                    messageId: activeConversation.last_message.id,
                });
            }
        }
    }, [activeConversation?.id, activeConversation?.last_message?.id, socket]);

    // Synchronize active conversation with URL params
    useEffect(() => {
        let isCancelled = false;
        const current = activeConvRef.current;

        // CASE 1: Real conversation with conversation_id
        if (cId) {
            const currentId = current?.id;
            const isMissingDetails = !current?.member_ids || current.member_ids.length === 0;

            if (currentId !== cId || isMissingDetails) {
                // O(1) direct query lookup from chatCache
                const cached = chatCache.getConversation(cId);
                if (cached) {
                    setActiveConversation(cached);
                    return;
                }

                // Fetch from API if not in cache
                conversationApi
                    .getConversationById(cId)
                    .then((conv) => {
                        if (!isCancelled) {
                            setActiveConversation(conv);
                            chatCache.setConversation(conv);
                        }
                    })
                    .catch((err) => {
                        if (!isCancelled) {
                            console.error("Không thể load hội thoại từ URL", err);
                            router.replace(fallbackRoute);
                        }
                    });
            }
        }
        // CASE 2: Draft conversation with receiver_id (new friend, not yet messaged, or self cloud)
        else if (receiverId) {
            const currentReceiverId = current?.receiver_id;
            const currentUserId = useAuthStore.getState().user?.id || "";
            const isSelf = Boolean(currentUserId && receiverId === currentUserId);

            const basePath = pathname.startsWith("/group-chat")
                ? "/group-chat"
                : pathname.startsWith("/direct-chat")
                ? "/direct-chat"
                : "/chat";

            if (isSelf) {
                // 1. O(1) check if self conversation exists in cache
                const selfConvId = chatCache.getSelfConversationId(currentUserId);
                if (selfConvId) {
                    const selfConv = chatCache.getConversation(selfConvId);
                    router.replace(`${basePath}?conversation_id=${selfConvId}`);
                    if (selfConv) setActiveConversation(selfConv);
                    return;
                }

                // 2. Fetch self conversation from server
                conversationApi
                    .getSelfConversation()
                    .then((selfConv) => {
                        if (!isCancelled && selfConv?.id) {
                            chatCache.setConversation(selfConv);
                            router.replace(`${basePath}?conversation_id=${selfConv.id}`);
                            setActiveConversation(selfConv);
                        }
                    })
                    .catch((err) => {
                        console.error("Không thể lấy Cloud của tôi:", err);
                        if (!isCancelled) {
                            const currentUser = useAuthStore.getState().user;
                            setActiveConversation({
                                _id: "",
                                id: "",
                                type: "self",
                                name: "Cloud của tôi",
                                avatar_url: currentUser?.avatar_url,
                                member_ids: [currentUserId],
                                members: [{ id: currentUserId } as any],
                                receiver_id: currentUserId,
                                is_active: true,
                            } as any);
                        }
                    });
            } else {
                // 1. O(1) check if 1-1 conversation already exists in cache for this user
                const directConvId = chatCache.getDirectConversationId(receiverId, currentUserId);

                if (directConvId) {
                    const existing = chatCache.getConversation(directConvId);
                    router.replace(`${basePath}?conversation_id=${directConvId}`);
                    if (existing) setActiveConversation(existing);
                } else if (currentReceiverId !== receiverId || !current) {
                    // Fetch target user metadata to build draft conversation
                    userApi
                        .getUserById(receiverId)
                        .then((targetUser) => {
                            if (!isCancelled) {
                                setActiveConversation({
                                    _id: "",
                                    id: "",
                                    type: "utu",
                                    name: targetUser.name || "Người dùng mới",
                                    avatar_url: targetUser.avatar_url,
                                    member_ids: [currentUserId, receiverId],
                                    members: [
                                        { id: currentUserId } as any,
                                        { id: receiverId } as any,
                                    ],
                                    receiver_id: receiverId,
                                    is_active: true,
                                } as any);
                            }
                        })
                        .catch((err) => {
                            if (!isCancelled) {
                                console.error("Không thể load user từ URL", err);
                                router.replace(fallbackRoute);
                            }
                        });
                }
            }
        }
        // CASE 3: No conversation selected in URL
        else {
            if (activeConvRef.current) {
                setActiveConversation(null);
            }
        }

        return () => {
            isCancelled = true;
        };
    }, [cId, receiverId, router, pathname, setActiveConversation, fallbackRoute]);

    return {
        activeConversation,
        isDraft: Boolean(receiverId && !cId),
        conversationId: cId,
        receiverId,
    };
};
