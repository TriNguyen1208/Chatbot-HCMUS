import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Users, User as UserIcon } from "lucide-react";

import { SearchResult } from "../../api/search.api";
import { conversationApi } from "../../api/conversation.api";
import { useChatStore } from "../../stores/chatStore";
import { useSearchStore } from "../../stores/searchStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useUserStore } from "../../stores/userStore";
import { Conversation } from "../../types";

const SearchItem = ({ item }: { item: SearchResult }) => {
    const { setActiveConversation } = useChatStore();
    const { setTargetMessageId, setSearchMode } = useSearchStore();
    const { user } = useAuthStore();
    const { users, requestUser } = useUserStore();
    const queryClient = useQueryClient();
    const router = useRouter();

    // If item is a conversation, resolve its full data and avatar_url
    const isConversation = item.search_type === 'conversation';
    const { data: convData } = useQuery<Conversation | null>({
        queryKey: ['conversation', item.id],
        queryFn: async () => {
            const res = await conversationApi.getConversationById(item.id);
            return (res as any).data || res;
        },
        enabled: isConversation && !!item.id,
        initialData: () => {
            if (!isConversation || !item.id) return undefined;
            const allCaches = queryClient.getQueriesData<{ pages: Conversation[][] }>({ queryKey: ['conversations'] });
            for (const [_, data] of allCaches) {
                if (data?.pages) {
                    const found = data.pages.flat().find(c => c.id === item.id);
                    if (found) return found;
                }
            }
            return undefined;
        },
        staleTime: 1000 * 60 * 5,
    });

    // In case it's a 1-1 conversation and needs otherMember's avatar
    const otherMemberId = convData?.member_ids?.find((m: string) => m !== user?.id) as string;
    const otherMember = users[otherMemberId];

    useEffect(() => {
        if (convData?.type === 'utu' && otherMemberId && !otherMember) {
            requestUser(otherMemberId);
        }
    }, [convData?.type, otherMemberId, otherMember, requestUser]);

    const displayAvatar = item.avatar_url 
        || convData?.avatar_url 
        || (convData?.type === 'utu' ? otherMember?.avatar_url : undefined);

    const displayName = item.name || convData?.name || otherMember?.name || "Cuộc trò chuyện";

    const fetchFullConversation = async (convId: string): Promise<Conversation | null> => {
        // 1. Check in existing query caches
        const allCaches = queryClient.getQueriesData<{ pages: Conversation[][] }>({ queryKey: ['conversations'] });
        for (const [_, data] of allCaches) {
            if (data?.pages) {
                const found = data.pages.flat().find(c => c.id === convId);
                if (found && found.member_ids && found.member_ids.length > 0) return found;
            }
        }
        // 2. Fetch from API
        try {
            const res = await conversationApi.getConversationById(convId);
            return (res as any).data || res;
        } catch (error) {
            console.error("Failed to fetch conversation details:", error);
            return null;
        }
    };

    const handleClick = async () => {
        if (item.search_type === 'user') {
            try {
                // For a 1-1 conversation, member_ids is just the other user's id.
                // The backend handles adding the current user.
                const res = await conversationApi.createDirectConversation([item.id]);
                const conversation = res.data || res;
                setActiveConversation({
                    id: conversation.id,
                    name: conversation.name,
                    avatar_url: conversation.avatar_url,
                    type: conversation.type || 'utu'
                } as any);
                router.push(`?conversation_id=${conversation.id}`);
                setSearchMode(false);
            } catch (error) {
                console.error("Failed to create or fetch direct conversation:", error);
            }
        } else if (item.search_type === 'conversation') {
            const convId = item.id;
            setSearchMode(false);
            const fullConv = (convData && convData.member_ids && convData.member_ids.length > 0)
                ? convData
                : await fetchFullConversation(convId);
            if (fullConv) {
                setActiveConversation(fullConv);
            }
            router.push(`?conversation_id=${convId}`);
        } else if (item.search_type === 'message' && item.conversation) {
            const convId = item.conversation.id;
            setTargetMessageId(item.id);
            setSearchMode(false);
            const fullConv = await fetchFullConversation(convId);
            if (fullConv) {
                setActiveConversation(fullConv);
            }
            router.push(`?conversation_id=${convId}`);
        }
    };

    if (item.search_type === 'user') {
        return (
            <div 
                onClick={handleClick}
                className="flex items-center gap-3 p-2 rounded-lg hover:bg-glass-panel cursor-pointer transition-colors group"
            >
                <div className="relative size-10 shrink-0 rounded-full bg-surface-solid border border-glass-border flex items-center justify-center overflow-hidden">
                    {item.avatar_url ? (
                        <img src={item.avatar_url} alt={item.name} className="w-full h-full object-cover" />
                    ) : (
                        <UserIcon size={20} className="text-brand-primary" />
                    )}
                </div>
                <div className="flex flex-col flex-1 min-w-0">
                    <span className="text-sm font-semibold text-txt-primary truncate group-hover:text-brand-primary transition-colors">
                        {item.name}
                    </span>
                    <span className="text-xs text-txt-extra truncate">User</span>
                </div>
            </div>
        );
    }

    if (item.search_type === 'conversation') {
        return (
            <div 
                onClick={handleClick}
                className="flex items-center gap-3 p-2 rounded-lg hover:bg-glass-panel cursor-pointer transition-colors group"
            >
                <div className="relative size-10 shrink-0 rounded-full bg-surface-solid border border-glass-border flex items-center justify-center overflow-hidden">
                    {displayAvatar ? (
                        <img src={displayAvatar} alt={displayName} className="w-full h-full object-cover" />
                    ) : (
                        <Users size={20} className="text-brand-primary" />
                    )}
                </div>
                <div className="flex flex-col flex-1 min-w-0">
                    <span className="text-sm font-semibold text-txt-primary truncate group-hover:text-brand-primary transition-colors">
                        {displayName}
                    </span>
                    <span className="text-xs text-txt-extra truncate">Conversation</span>
                </div>
            </div>
        );
    }

    if (item.search_type === 'message' && item.sender && item.conversation) {
        return (
            <div 
                onClick={handleClick}
                className="flex items-start gap-3 p-2 rounded-lg hover:bg-glass-panel cursor-pointer transition-colors group"
            >
                <div className="mt-1">
                    <div className="relative size-10 shrink-0 rounded-full bg-surface-solid border border-glass-border flex items-center justify-center overflow-hidden">
                        {item.sender.avatar_url ? (
                            <img src={item.sender.avatar_url} alt={item.sender.name} className="w-full h-full object-cover" />
                        ) : (
                            <UserIcon size={20} className="text-brand-primary" />
                        )}
                    </div>
                </div>
                <div className="flex flex-col flex-1 min-w-0 gap-0.5">
                    <div className="flex items-center justify-between">
                        <span className="text-sm font-semibold text-txt-primary truncate">
                            {item.sender.name}
                        </span>
                    </div>
                    <p className="text-sm text-txt-primary line-clamp-2">
                        {item.content || (item as any).text}
                    </p>
                    <div className="flex items-center gap-1 mt-1 text-xs text-brand-primary/80 truncate">
                        <span className="font-medium truncate">{item.conversation.name}</span>
                    </div>
                </div>
            </div>
        );
    }

    return null;
};

export default SearchItem;
