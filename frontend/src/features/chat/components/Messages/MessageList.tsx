"use client";
import { useEffect } from "react";
import { useMessageList } from "@/features/chat/hooks/useMessageList";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useSearchStore } from "@/features/chat/stores/searchStore";
import { useQueryClient } from "@tanstack/react-query";
import { messageApi } from "@/features/chat/api/message.api";
import MessageItem from "./MessageItem";
import { Loader2 } from "lucide-react";

const MessageList = () => {
    const {
        messages,
        isLoadingMessages,
        hasMoreMessages,
        ref
    } = useMessageList();
    const { user } = useAuthStore();
    const activeConversation = useChatStore(state => state.activeConversation);
    const typingUsersMap = useChatStore(state => state.typingUsers);

    const convId = activeConversation?.id;
    const currentTypingUsers = (convId ? (typingUsersMap[convId] || []) : []).filter(u => u.userId !== user?.id);

    const { targetMessageId, setTargetMessageId } = useSearchStore();
    const queryClient = useQueryClient();

    useEffect(() => {
        const fetchContextAndScroll = async () => {
            if (targetMessageId && convId) {
                try {
                    const res = await messageApi.getContextMessages(convId, targetMessageId);
                    const contextMessages = Array.isArray(res.data) ? res.data : res;
                    
                    // Replace the react-query cache with context messages
                    queryClient.setQueryData(['messages', convId], (old: any) => {
                        return {
                            pages: [contextMessages],
                            pageParams: [undefined]
                        };
                    });

                    // Scroll to the message element
                    setTimeout(() => {
                        const el = document.getElementById(`msg-${targetMessageId}`);
                        if (el) {
                            el.scrollIntoView({ behavior: 'smooth', block: 'center' });
                            // Highlight effect
                            el.classList.add('bg-brand-primary/20', 'transition-colors', 'duration-500');
                            setTimeout(() => {
                                el.classList.remove('bg-brand-primary/20');
                            }, 3000);
                        }
                    }, 300); // Wait for render
                    
                    // Clear target message after jumping
                    setTargetMessageId(null);
                } catch (error) {
                    console.error("Failed to load context messages:", error);
                }
            }
        };
        fetchContextAndScroll();
    }, [targetMessageId, convId, queryClient, setTargetMessageId]);

    return (
        <div className="flex-1 overflow-y-auto p-4 flex flex-col-reverse space-y-reverse space-y-4 relative">
            {currentTypingUsers.length > 0 && (
                <div className="flex items-center gap-2 self-start animate-in fade-in zoom-in-95 duration-300">
                    <div className="bg-surface-solid border border-glass-border rounded-full px-4 py-2 flex items-center gap-2 shadow-sm">
                        <span className="text-xs font-medium text-txt-extra">
                            {currentTypingUsers.map(u => u.name).join(', ')} đang gõ
                        </span>
                        <div className="flex gap-1 items-center">
                            <span className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-bounce [animation-delay:-0.3s]"></span>
                            <span className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-bounce [animation-delay:-0.15s]"></span>
                            <span className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-bounce"></span>
                        </div>
                    </div>
                </div>
            )}
            
            {messages.map((msg) => (
                <MessageItem
                    key={msg.id || (msg as any).id}
                    message={msg}
                />
            ))}

            {hasMoreMessages && messages.length > 0 && (
                <div ref={ref} className="h-4 flex items-center justify-center shrink-0">
                    {isLoadingMessages && <Loader2 className="w-5 h-5 animate-spin text-gray-400" />}
                </div>
            )}
        </div>
    );
};

export default MessageList;
