import { useEffect } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import { useQueryClient } from "@tanstack/react-query";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { conversationApi } from "@/features/chat/api/conversation.api";
import { userApi } from "@/features/chat/api/user.api";
import { Conversation } from "../types";

export const useChatScreen = (type?: "utu" | "group" | "all") => {
  const { activeConversation, setActiveConversation } = useChatStore();
  const searchParams = useSearchParams();
  const router = useRouter();
  const queryClient = useQueryClient();

  const fallbackRoute = type === "group" ? "/group-chat" : type === "utu" ? "/direct-chat" : "/chat";

  const cId = searchParams.get("conversation_id");
  const receiverId = searchParams.get("receiver_id");

  useEffect(() => {
    if (cId) {
        const currentActiveId = activeConversation?._id || (activeConversation as any)?.id;
        if (currentActiveId !== cId) {
            const allCaches = queryClient.getQueriesData<any>({ queryKey: ['conversations'] });
            let conversations: Conversation[] = [];
            allCaches.forEach(([_, data]) => {
                if (data?.pages) {
                    conversations = conversations.concat(data.pages.flat());
                }
            });
            const found = conversations.find(c => (c._id === cId || (c as any).id === cId));
            
            if (found) {
                setActiveConversation(found);
            } else {
                conversationApi.getConversationById(cId).then(res => {
                    const conv = res.data || res;
                    setActiveConversation(conv);
                }).catch((err) => {
                    console.error("Không thể load hội thoại từ URL", err);
                    router.replace(fallbackRoute);
                });
            }
        }
    } 
    else if (receiverId) {
        const currentActiveReceiverId = (activeConversation as any)?.receiver_id;
        
        // Find in all possible conversation caches ('utu', 'group', or undefined)
        const allCaches = queryClient.getQueriesData<any>({ queryKey: ['conversations'] });
        let conversations: Conversation[] = [];
        allCaches.forEach(([_, data]) => {
            if (data?.pages) {
                conversations = conversations.concat(data.pages.flat());
            }
        });
        
        const found = conversations.find(c => c.type === 'utu' && c.members?.some(m => m.id === receiverId));
        
        if (found) {
            const currentActiveId = activeConversation?._id || (activeConversation as any)?.id;
            if (currentActiveId !== (found._id || (found as any).id)) {
                setActiveConversation(found);
            }
        } else if (currentActiveReceiverId !== receiverId) {
            userApi.getUserById(receiverId).then(res => {
                const targetUser = res.data || res;
                setActiveConversation({
                    _id: '',
                    type: 'utu',
                    name: targetUser.name || 'Người dùng mới',
                    avatar_url: targetUser.avatar_url,
                    members: [
                        { id: useAuthStore.getState().user?.id || '' } as any,
                        { id: receiverId } as any
                    ],
                    receiver_id: receiverId,
                    is_active: true
                } as any);
            }).catch((err) => {
                console.error("Không thể load user từ URL", err);
                router.replace(fallbackRoute);
            });
        }
    }
    else {
        if (activeConversation) {
            setActiveConversation(null);
        }
    }
  }, [cId, receiverId, router, activeConversation, queryClient, setActiveConversation, fallbackRoute]);

  return { activeConversation };
};
