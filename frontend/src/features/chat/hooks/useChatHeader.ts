import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { useQueryClient } from "@tanstack/react-query";
import { useChatStore } from "../stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useUserStore } from "../stores/userStore";
import { conversationApi } from "@/features/chat/api/conversation.api";
import { DEFAULT_AVATAR } from "@/utils/constants";
import { useModalStore } from "../stores/modalStore";

export const useChatHeader = () => {
    const { activeConversation, setActiveConversation } = useChatStore();
    const { user } = useAuthStore();
    const { users, requestUser } = useUserStore();
    const router = useRouter();
    const queryClient = useQueryClient();
    const setAssignAdminModalOpen = useModalStore((state) => state.setAssignAdminModalOpen);

    useEffect(() => {
        if (activeConversation?.member_ids) {
            activeConversation.member_ids.forEach((id: string) => {
                if (!users[id]) requestUser(id);
            });
        }
        
    }, [activeConversation?.member_ids, users, requestUser]);

    const otherMemberId = activeConversation?.member_ids?.find(m => m !== user?.id) as string;
    const otherMember = users[otherMemberId];
    
    const displayName = activeConversation?.name || otherMember?.name || "Cloud của tôi";
    const displayAvatar = activeConversation?.avatar_url || otherMember?.avatar_url || DEFAULT_AVATAR;
    
    const isAdmin = activeConversation?.admin_ids?.some(adminId => adminId === user?.id);
    const adminCount = activeConversation?.admin_ids?.length || 0;
    const memberCount = activeConversation?.member_ids?.length || 0;

    const handleLeaveGroup = async () => {
        if (!activeConversation) return;

        // If sole admin in a group with > 1 member, prompt to assign admin first
        if (isAdmin && adminCount <= 1 && memberCount > 1) {
            alert("Bạn là Admin duy nhất trong nhóm. Vui lòng cấp quyền Admin cho người khác trước khi rời nhóm!");
            setAssignAdminModalOpen(true);
            return;
        }

        try {
            const convId = activeConversation.id as string;
            await conversationApi.leaveGroup(convId);

            queryClient.invalidateQueries({ queryKey: ["conversations"] });
            setActiveConversation(null);
            router.push("/group-chat");
        } catch (error: unknown) {
            console.error("Lỗi rời nhóm:", error);
            alert((error as Error)?.message || "Đã xảy ra lỗi" || "Không thể rời khỏi nhóm");
        }
    };

    return { 
        activeConversation,
        displayName,
        displayAvatar,
        isAdmin,
        handleLeaveGroup,
        otherMember,
        isOnline: activeConversation?.type === 'utu' 
            ? (otherMember?.is_online || false) 
            : (activeConversation?.member_ids?.some((id: string) => id !== user?.id && users[id]?.is_online) || false)
    };
};
