import { useEffect, useRef } from 'react';
import { io, Socket } from 'socket.io-client';
import { useQueryClient } from '@tanstack/react-query';
import { useRouter } from 'next/navigation';
import { env } from '@/config/env';
import { Message, Conversation } from '../types';
import { useChatStore } from '../stores/chatStore';
import { useAuthStore } from '@/features/auth/stores/authStore';
import { useUserStore } from '../stores/userStore';

export const useChatSocket = () => {
    const socketRef = useRef<Socket | null>(null);
    const queryClient = useQueryClient();
    const router = useRouter();

    useEffect(() => {
        const socket = io(env.apiUrl, {
            withCredentials: true,
            transports: ['websocket', 'polling'],
        });

        socketRef.current = socket;

        socket.on('connect', () => {
            console.log('Connected to Chat Socket');

            // Advanced Heartbeat mechanism with jitter to avoid simultaneous pings
            const pingInterval = setInterval(() => {
                const jitter = Math.random() * 2000; // 0 to 2 seconds jitter
                setTimeout(() => {
                    if (socket.connected) {
                        socket.emit('ping');
                    }
                }, jitter);
            }, 29000); // Ping every 29 seconds

            socket.once('disconnect', () => {
                clearInterval(pingInterval);
            });
        });

        socket.on('user_online', (data: { userId: string }) => {
            useUserStore.getState().updateUserPresence(data.userId, true);
        });

        socket.on('user_offline', (data: { userId: string, last_active: string }) => {
            useUserStore.getState().updateUserPresence(data.userId, false, data.last_active);
        });        
        socket.on('typing', (data: { conversationId: string, userId: string, name: string }) => {
            useChatStore.getState().addTypingUser(data.conversationId, data.userId, data.name);
        });

        socket.on('stop_typing', (data: { conversationId: string, userId: string }) => {
            useChatStore.getState().removeTypingUser(data.conversationId, data.userId);
        });

        // 4. LẮNG NGHE SỰ KIỆN: CÓ TIN NHẮN MỚI
        socket.on('new_message', (message: Message) => {
            console.log("Frontend received new_message:", message);
            queryClient.setQueryData(['messages', message.conversation_id], (oldData: { pages: any[], pageParams: any[] } | undefined) => {
                // Nếu chưa có cache (trường hợp tạo mới), tự tạo một cache ảo
                if (!oldData) {
                    return {
                        pages: [[message]],
                        pageParams: [undefined]
                    };
                }

                const newPages = [...oldData.pages];
                let isUpdated = false;

                // Cập nhật lại tin nhắn nếu đã tồn tại (dùng cho trường hợp Video xử lý xong)
                newPages[0] = newPages[0]?.map((m: Message) => {
                    if (m.id === message.id) {
                        isUpdated = true;
                        return message; // Thay thế bằng tin nhắn mới nhất
                    }
                    return m;
                }) || [];

                // Nếu chưa tồn tại thì thêm mới vào đầu
                if (!isUpdated) {
                    newPages[0] = [message, ...newPages[0]];
                }

                return { ...oldData, pages: newPages };
            });

            queryClient.setQueriesData({ queryKey: ['conversations'] }, (oldData: { pages: any[], pageParams: any[] } | undefined) => {
                if (!oldData) return oldData;

                let updatedConv: any = null;
                const newPages = oldData.pages.map((page: any[]) => {
                    return page.filter((conv: Conversation) => {
                        // Nếu tìm thấy đúng cuộc hội thoại mà tin nhắn mới vừa bay tới
                        if (conv.id === message.conversation_id || conv.id === message.conversation_id) {
                            updatedConv = {
                                ...conv,
                                last_message: message,
                                updated_at: message.created_at || new Date().toISOString()
                            };
                            return false; // Bỏ qua khỏi vị trí hiện tại
                        }
                        return true;
                    });
                });

                if (updatedConv && newPages.length > 0) {
                    // Chèn lên đầu trang đầu tiên
                    newPages[0] = [updatedConv, ...newPages[0]];
                }

                return { ...oldData, pages: newPages };
            });
        });

        // 5. LẮNG NGHE SỰ KIỆN: CÓ NGƯỜI SỬA TIN NHẮN
        socket.on('message_edited', (data: { conversation_id: string, messageId: string, content: string, updated_at: string }) => {
            queryClient.setQueryData(['messages', data.conversation_id], (oldData: { pages: any[], pageParams: any[] } | undefined) => {
                if (!oldData) return oldData;
                const newPages = oldData.pages.map((page: any[]) =>
                    page.map((msg: Message) => (msg.id === data.messageId || msg.id === data.messageId) ? { ...msg, content: data.content, updated_at: data.updated_at, is_edited: true } : msg)
                );
                return { ...oldData, pages: newPages };
            });

            queryClient.setQueriesData({ queryKey: ['conversations'] }, (oldData: { pages: any[], pageParams: any[] } | undefined) => {
                if (!oldData) return oldData;
                const newPages = oldData.pages.map((page: any[]) =>
                    page.map((conv: Conversation) => {
                        if ((conv.id === data.conversation_id || conv.id === data.conversation_id) &&
                            conv.last_message &&
                            (conv.last_message.id === data.messageId || conv.last_message.id === data.messageId)) {
                            return {
                                ...conv,
                                last_message: { ...conv.last_message, content: data.content, updated_at: data.updated_at, is_edited: true }
                            };
                        }
                        return conv;
                    })
                );
                return { ...oldData, pages: newPages };
            });
        });

        socket.on('message_reaction_updated', (data: { conversation_id: string, message_id: string, reactions: any[] }) => {
            console.log(data)
            queryClient.setQueryData(['messages', data.conversation_id], (oldData: { pages: any[], pageParams: any[] } | undefined) => {
                if (!oldData) return oldData;
                const newPages = oldData.pages.map((page: any[]) =>
                    page.map((msg: Message) => (msg.id === data.message_id || msg.id === data.message_id) ? { ...msg, reactions: data.reactions } : msg)
                );
                return { ...oldData, pages: newPages };
            });

            queryClient.setQueriesData({ queryKey: ['conversations'] }, (oldData: { pages: any[], pageParams: any[] } | undefined) => {
                if (!oldData) return oldData;
                const newPages = oldData.pages.map((page: any[]) =>
                    page.map((conv: Conversation) => {
                        if ((conv.id === data.conversation_id || conv.id === data.conversation_id) &&
                            conv.last_message &&
                            (conv.last_message.id === data.message_id || conv.last_message.id === data.message_id)) {
                            return {
                                ...conv,
                                last_message: { ...conv.last_message, reactions: data.reactions }
                            };
                        }
                        return conv;
                    })
                );
                return { ...oldData, pages: newPages };
            });
        });

        // 6. LẮNG NGHE SỰ KIỆN: CÓ NGƯỜI THU HỒI TIN NHẮN
        socket.on('message_recalled', (data: { conversation_id: string, messageId: string }) => {
            queryClient.setQueryData(['messages', data.conversation_id], (oldData: { pages: any[], pageParams: any[] } | undefined) => {
                if (!oldData) return oldData;
                const newPages = oldData.pages.map((page: any[]) =>
                    page.map((msg: Message) => (msg.id === data.messageId || msg.id === data.messageId) ? { ...msg, status: 'recalled' } : msg)
                );
                return { ...oldData, pages: newPages };
            });

            queryClient.setQueriesData({ queryKey: ['conversations'] }, (oldData: { pages: any[], pageParams: any[] } | undefined) => {
                if (!oldData) return oldData;
                const newPages = oldData.pages.map((page: any[]) =>
                    page.map((conv: Conversation) => {
                        if ((conv.id === data.conversation_id || conv.id === data.conversation_id) &&
                            conv.last_message &&
                            (conv.last_message.id === data.messageId || conv.last_message.id === data.messageId)) {
                            return {
                                ...conv,
                                last_message: { ...conv.last_message, status: 'recalled' }
                            };
                        }
                        return conv;
                    })
                );
                return { ...oldData, pages: newPages };
            });
        });

        // 7. LẮNG NGHE SỰ KIỆN: BẠN ĐƯỢC THÊM VÀO NHÓM MỚI (Hoặc có người lạ vừa nhắn tin)
        socket.on('new_conversation', (conversation: Conversation) => {
            console.log("Frontend received new_conversation:", conversation);
            const updateCache = (queryKey: string[]) => {
                queryClient.setQueryData(queryKey, (oldData: { pages: any[], pageParams: any[] } | undefined) => {
                    if (!oldData) {
                        return { pages: [[conversation]], pageParams: [undefined] };
                    }
                    const newPages = [...oldData.pages];

                    const exists = newPages[0]?.some((c: Conversation) => c.id === conversation.id);
                    if (exists) return oldData;

                    newPages[0] = [conversation, ...(newPages[0] || [])];
                    return { ...oldData, pages: newPages };
                });
            };

            updateCache(['conversations']);
            updateCache(['conversations', conversation.type]);

            // Fix lỗi người dùng tạo đoạn chat mới lần đầu tiên không nhảy URL và ActiveConversation
            const { activeConversation, setActiveConversation } = useChatStore.getState();
            if (activeConversation && !activeConversation.id && !activeConversation.id) {
                if (conversation.type === 'utu' && conversation.member_ids?.some((m: string) => m === activeConversation.receiver_id)) {
                    setActiveConversation(conversation);
                    router.replace(`?conversation_id=${conversation.id}`);
                }
            }
        });

        // 8. LẮNG NGHE SỰ KIỆN: KICK HOẶC RỜI NHÓM
        socket.on('members_kicked', (data: { conversationId: string; memberIds: string[] }) => {
            queryClient.invalidateQueries({ queryKey: ['conversations'] });
            const { user } = useAuthStore.getState();
            const { activeConversation, setActiveConversation } = useChatStore.getState();

            if (user?.id && data.memberIds.includes(user.id)) {
                const currentActiveId = activeConversation?.id || activeConversation?.id;
                if (currentActiveId === data.conversationId) {
                    setActiveConversation(null);
                    router.push('/group-chat');
                }
            }
        });

        socket.on('member_left', (data: { conversationId: string; userId: string }) => {
            queryClient.invalidateQueries({ queryKey: ['conversations'] });
            const { user } = useAuthStore.getState();
            const { activeConversation, setActiveConversation } = useChatStore.getState();

            if (user?.id && data.userId === user.id) {
                const currentActiveId = activeConversation?.id || activeConversation?.id;
                if (currentActiveId === data.conversationId) {
                    setActiveConversation(null);
                    router.push('/group-chat');
                }
            }
        });

        // 9. Cleanup function: Ngắt kết nối socket khi người dùng đóng trang
        return () => {
            socket.disconnect();
            socketRef.current = null;
        };
    }, [queryClient, router]); // Chạy lại Effect nếu biến queryClient hoặc router thay đổi (rất hiếm khi xảy ra)

    // Trả về biến socket ra ngoài để các Component khác có thể dùng lệnh socket.emit(...) nếu cần
    return socketRef.current;
};
