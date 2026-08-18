import { useEffect, useRef } from 'react';
import { io, Socket } from 'socket.io-client';
import { useQueryClient } from '@tanstack/react-query';
import { useRouter } from 'next/navigation';
import { env } from '@/config/env';
import { Message, Conversation } from '../types';
import { useChatStore } from '../stores/chatStore';
import { useAuthStore } from '@/features/auth/stores/authStore';

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
        });

        // 4. LẮNG NGHE SỰ KIỆN: CÓ TIN NHẮN MỚI
        socket.on('new_message', (message: Message) => {
            console.log("Frontend received new_message:", message);
            queryClient.setQueryData(['messages', message.conversation_id], (oldData: any) => {
                // Nếu chưa có cache (trường hợp tạo mới), tự tạo một cache ảo
                console.log()
                if (!oldData) {
                    return {
                        pages: [[message]],
                        pageParams: [undefined]
                    };
                }

                const newPages = [...oldData.pages];
                const exists = newPages[0]?.some((m: any) => m._id === message._id);
                if (exists) return oldData;

                newPages[0] = [message, ...(newPages[0] || [])];

                return { ...oldData, pages: newPages };
            });

            queryClient.setQueriesData({ queryKey: ['conversations'] }, (oldData: any) => {
                if (!oldData) return oldData;

                const newPages = oldData.pages.map((page: any[]) =>
                    page.map((conv: any) => {
                        // Nếu tìm thấy đúng cuộc hội thoại mà tin nhắn mới vừa bay tới
                        if (conv._id === message.conversation_id || (conv as any).id === message.conversation_id) {
                            return {
                                ...conv,
                                last_message: message
                            };
                        }
                        return conv;
                    })
                );
                return { ...oldData, pages: newPages };
            });
        });

        // 5. LẮNG NGHE SỰ KIỆN: CÓ NGƯỜI SỬA TIN NHẮN
        socket.on('message_edited', (data: any) => {
            queryClient.setQueryData(['messages', data.conversation_id], (oldData: any) => {
                if (!oldData) return oldData;
                const newPages = oldData.pages.map((page: any[]) =>
                    page.map((msg: any) => (msg._id === data.messageId || msg.id === data.messageId) ? { ...msg, content: data.content, updated_at: data.updated_at, is_edited: true } : msg)
                );
                return { ...oldData, pages: newPages };
            });

            queryClient.setQueriesData({ queryKey: ['conversations'] }, (oldData: any) => {
                if (!oldData) return oldData;
                const newPages = oldData.pages.map((page: any[]) =>
                    page.map((conv: any) => {
                        if ((conv._id === data.conversation_id || (conv as any).id === data.conversation_id) &&
                            conv.last_message &&
                            (conv.last_message._id === data.messageId || conv.last_message.id === data.messageId)) {
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

        // 6. LẮNG NGHE SỰ KIỆN: CÓ NGƯỜI THU HỒI TIN NHẮN
        socket.on('message_recalled', (data: any) => {
            queryClient.setQueryData(['messages', data.conversation_id], (oldData: any) => {
                if (!oldData) return oldData;
                const newPages = oldData.pages.map((page: any[]) =>
                    page.map((msg: any) => (msg._id === data.messageId || msg.id === data.messageId) ? { ...msg, status: 'recalled' } : msg)
                );
                return { ...oldData, pages: newPages };
            });

            queryClient.setQueriesData({ queryKey: ['conversations'] }, (oldData: any) => {
                if (!oldData) return oldData;
                const newPages = oldData.pages.map((page: any[]) =>
                    page.map((conv: any) => {
                        if ((conv._id === data.conversation_id || (conv as any).id === data.conversation_id) &&
                            conv.last_message &&
                            (conv.last_message._id === data.messageId || conv.last_message.id === data.messageId)) {
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
            const updateCache = (queryKey: any[]) => {
                queryClient.setQueryData(queryKey, (oldData: any) => {
                    if (!oldData) {
                        return { pages: [[conversation]], pageParams: [undefined] };
                    }
                    const newPages = [...oldData.pages];

                    const exists = newPages[0]?.some((c: any) => c._id === conversation._id);
                    if (exists) return oldData;

                    newPages[0] = [conversation, ...(newPages[0] || [])];
                    return { ...oldData, pages: newPages };
                });
            };

            updateCache(['conversations']);
            updateCache(['conversations', conversation.type]);

            // Fix lỗi người dùng tạo đoạn chat mới lần đầu tiên không nhảy URL và ActiveConversation
            const { activeConversation, setActiveConversation } = useChatStore.getState();
            if (activeConversation && !activeConversation._id && !(activeConversation as any).id) {
                if (conversation.type === 'utu' && conversation.members?.some((m: any) => m.id === (activeConversation as any).receiver_id)) {
                    setActiveConversation(conversation);
                    router.replace(`?conversation_id=${conversation._id}`);
                }
            }
        });

        // 8. LẮNG NGHE SỰ KIỆN: KICK HOẶC RỜI NHÓM
        socket.on('members_kicked', (data: { conversationId: string; memberIds: string[] }) => {
            queryClient.invalidateQueries({ queryKey: ['conversations'] });
            const { user } = useAuthStore.getState();
            const { activeConversation, setActiveConversation } = useChatStore.getState();

            if (user?.id && data.memberIds.includes(user.id)) {
                const currentActiveId = activeConversation?._id || (activeConversation as any)?.id;
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
                const currentActiveId = activeConversation?._id || (activeConversation as any)?.id;
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
