"use client";
import { useEffect, useMemo } from "react";
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

    const watermarksByMessageId = useMemo(() => {
        const result: Record<string, { type: 'delivered' | 'read', userId: string }[]> = {};
        const rawWatermarks = activeConversation?.watermarks || [];
        
        // Deduplicate watermarks by userId
        const watermarksMap = new Map<string, { user_id: string; last_delivered_msg_id?: string | null; last_read_msg_id?: string | null }>();
        rawWatermarks.forEach(w => {
            if (w.user_id && w.user_id !== user?.id) {
                const existing = watermarksMap.get(w.user_id);
                if (!existing) {
                    watermarksMap.set(w.user_id, { ...w });
                } else {
                    watermarksMap.set(w.user_id, {
                        user_id: w.user_id,
                        last_delivered_msg_id: w.last_delivered_msg_id || existing.last_delivered_msg_id,
                        last_read_msg_id: w.last_read_msg_id || existing.last_read_msg_id,
                    });
                }
            }
        });

        watermarksMap.forEach(w => {
            // Find index of the latest message read by this user
            const readIdx = w.last_read_msg_id 
                ? messages.findIndex(m => m.id === w.last_read_msg_id || (m as any)._id === w.last_read_msg_id) 
                : -1;

            if (readIdx !== -1) {
                // Only attach the read avatar to the exact latest message read by this user
                const msgId = messages[readIdx].id || (messages[readIdx] as any)._id;
                if (!result[msgId]) result[msgId] = [];
                if (!result[msgId].some(r => r.userId === w.user_id)) {
                    result[msgId].push({ type: 'read', userId: w.user_id });
                }
            } else {
                // If user hasn't read any message yet, check delivered
                const deliveredIdx = w.last_delivered_msg_id 
                    ? messages.findIndex(m => m.id === w.last_delivered_msg_id || (m as any)._id === w.last_delivered_msg_id) 
                    : -1;

                if (deliveredIdx !== -1) {
                    const msgId = messages[deliveredIdx].id || (messages[deliveredIdx] as any)._id;
                    if (!result[msgId]) result[msgId] = [];
                    if (!result[msgId].some(r => r.userId === w.user_id)) {
                        result[msgId].push({ type: 'delivered', userId: w.user_id });
                    }
                }
            }
        });

        return result;
    }, [activeConversation?.watermarks, user?.id, messages]);

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
            
            {messages.map((msg, index) => (
                <MessageItem
                    key={msg.id || (msg as any).id}
                    message={msg}
                    watermarks={watermarksByMessageId[msg.id || '']}
                    isLastMessage={index === 0}
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
