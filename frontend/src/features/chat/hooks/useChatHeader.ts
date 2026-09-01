import { useState, useRef, useEffect } from "react";
import { useRouter } from "next/navigation";
import { useQueryClient } from "@tanstack/react-query";
import { useChatStore } from "../stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useUserStore } from "../stores/userStore";
import { conversationApi } from "@/features/chat/api/conversation.api";
import { DEFAULT_AVATAR } from "@/utils/constants";

export const useChatHeader = () => {
    const { activeConversation, setActiveConversation } = useChatStore();
    const { user } = useAuthStore();
    const { users, requestUser } = useUserStore();
    const router = useRouter();
    const queryClient = useQueryClient();

    const [showMenu, setShowMenu] = useState(false);
    const [isCreateGroupOpen, setIsCreateGroupOpen] = useState(false);
    const [isKickModalOpen, setIsKickModalOpen] = useState(false);
    const [isAssignAdminModalOpen, setIsAssignAdminModalOpen] = useState(false);
    const [errorMsg, setErrorMsg] = useState("");

    const menuRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        if (activeConversation?.member_ids) {
            activeConversation.member_ids.forEach((id: string) => {
                if (!users[id]) requestUser(id);
            });
        }
        
    }, [activeConversation?.member_ids, users, requestUser]);

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
                setShowMenu(false);
            }
        };
        document.addEventListener("mousedown", handleClickOutside);
        return () => document.removeEventListener("mousedown", handleClickOutside);
    }, []);

    const otherMemberId = activeConversation?.member_ids?.find(m => m !== user?.id) as string;
    const otherMember = users[otherMemberId];
    
    const displayName = activeConversation?.name || otherMember?.name || "Unknown";
    const displayAvatar = activeConversation?.avatar_url || otherMember?.avatar_url || DEFAULT_AVATAR;
    
    const isAdmin = activeConversation?.admin_ids?.some(adminId => adminId === user?.id);
    const adminCount = activeConversation?.admin_ids?.length || 0;
    const memberCount = activeConversation?.member_ids?.length || 0;

    const handleOpenCreateGroup = () => {
        setIsCreateGroupOpen(true);
        setShowMenu(false);
    };

    const handleCloseCreateGroup = () => {
        setIsCreateGroupOpen(false);
    };

    const handleOpenKickModal = () => {
        setIsKickModalOpen(true);
        setShowMenu(false);
    };

    const handleCloseKickModal = () => {
        setIsKickModalOpen(false);
    };

    const handleOpenAssignAdminModal = () => {
        setIsAssignAdminModalOpen(true);
        setShowMenu(false);
    };

    const handleCloseAssignAdminModal = () => {
        setIsAssignAdminModalOpen(false);
    };

    const handleLeaveGroup = async () => {
        if (!activeConversation) return;

        // If sole admin in a group with > 1 member, prompt to assign admin first
        if (isAdmin && adminCount <= 1 && memberCount > 1) {
            alert("Bạn là Admin duy nhất trong nhóm. Vui lòng cấp quyền Admin cho người khác trước khi rời nhóm!");
            setIsAssignAdminModalOpen(true);
            setShowMenu(false);
            return;
        }

        try {
            const convId = activeConversation.id as string;
            await conversationApi.leaveGroup(convId);

            queryClient.invalidateQueries({ queryKey: ["conversations"] });
            setActiveConversation(null);
            setShowMenu(false);
            router.push("/group-chat");
        } catch (error: unknown) {
            console.error("Lỗi rời nhóm:", error);
            alert((error as Error)?.message || "Đã xảy ra lỗi" || "Không thể rời khỏi nhóm");
        }
    };

    return { 
        activeConversation,
        showMenu,
        setShowMenu,
        isCreateGroupOpen,
        handleOpenCreateGroup,
        handleCloseCreateGroup,
        isKickModalOpen,
        handleOpenKickModal,
        handleCloseKickModal,
        isAssignAdminModalOpen,
        handleOpenAssignAdminModal,
        handleCloseAssignAdminModal,
        menuRef,
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
