import { useEffect, useRef } from "react";
import { useSearchParams, useRouter, usePathname } from "next/navigation";
import { useQueryClient } from "@tanstack/react-query";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { conversationApi } from "@/features/chat/api/conversation.api";
import { userApi } from "@/features/chat/api/user.api";
import { Conversation } from "@/features/chat/types";
import { useSocketContext } from "@/providers/SocketProvider";

/**
 * Custom hook establishing the Single Source of Truth for active chat conversations.
 * - URL (searchParams) determines WHICH conversation is open (`conversation_id` vs `receiver_id`).
 * - TanStack Query manages server cache and entity state.
 * - Zustand maintains UI sync for backward compatibility with existing components.
 */
export const useActiveConversation = (type?: "utu" | "group" | "all") => {
    const { activeConversation, setActiveConversation } = useChatStore();
    const searchParams = useSearchParams();
    const router = useRouter();
    const pathname = usePathname();
    const queryClient = useQueryClient();
    const { socket } = useSocketContext();

    const fallbackRoute = type === "group" ? "/group-chat" : type === "utu" ? "/direct-chat" : "/chat";

    const cId = searchParams.get("conversation_id");
    const receiverId = searchParams.get("receiver_id");
    const activeConvRef = useRef(activeConversation);
    activeConvRef.current = activeConversation;

    // Helper: Find conversation in TanStack Query cache without redundant re-allocations
    const findInCache = (predicate: (c: Conversation) => boolean): Conversation | undefined => {
        const allCaches = queryClient.getQueriesData<{ pages: Conversation[][] }>({ queryKey: ['conversations'] });
        for (const [_, data] of allCaches) {
            if (data?.pages) {
                for (const page of data.pages) {
                    const match = page.find(predicate);
                    if (match) return match;
                }
            }
        }
        return undefined;
    };

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
                // Check direct individual query cache first
                const cached = queryClient.getQueryData<Conversation>(["conversation", cId]);
                if (cached) {
                    setActiveConversation(cached);
                    return;
                }

                // Check list queries cache
                const found = findInCache((c) => c.id === cId);
                if (found) {
                    setActiveConversation(found);
                    queryClient.setQueryData(["conversation", cId], found);
                } else {
                    conversationApi
                        .getConversationById(cId)
                        .then((conv) => {
                            if (!isCancelled) {
                                setActiveConversation(conv);
                                queryClient.setQueryData(["conversation", cId], conv);
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
        }
        // CASE 2: Draft conversation with receiver_id (new friend, not yet messaged)
        else if (receiverId) {
            const currentReceiverId = current?.receiver_id;

            // Check if a 1-1 conversation already exists in cache for this user
            const existing = findInCache((c) => c.type === "utu" && Boolean(c.member_ids?.includes(receiverId)));

            if (existing) {
                // If it already exists, automatically upgrade URL to conversation_id
                const basePath = pathname.startsWith("/group-chat")
                    ? "/group-chat"
                    : pathname.startsWith("/direct-chat")
                    ? "/direct-chat"
                    : "/chat";
                router.replace(`${basePath}?conversation_id=${existing.id}`);
                setActiveConversation(existing);
                queryClient.setQueryData(["conversation", existing.id], existing);
            } else if (currentReceiverId !== receiverId || !current) {
                // Fetch target user metadata to build draft conversation
                userApi
                    .getUserById(receiverId)
                    .then((targetUser) => {
                        if (!isCancelled) {
                            const currentUserId = useAuthStore.getState().user?.id || "";
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
        // CASE 3: No conversation selected in URL
        else {
            if (activeConvRef.current) {
                setActiveConversation(null);
            }
        }

        return () => {
            isCancelled = true;
        };
    }, [cId, receiverId, router, pathname, queryClient, setActiveConversation, fallbackRoute]);

    return {
        activeConversation,
        isDraft: Boolean(receiverId && !cId),
        conversationId: cId,
        receiverId,
    };
};
