import { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import { useQueryClient } from "@tanstack/react-query";
import { userApi } from "@/features/chat/api/user.api";
import { User } from "@/features/chat/types";
import { conversationApi } from "@/features/chat/api/conversation.api";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useUserStore } from "@/features/chat/stores/userStore";

export const useCreateGroupModal = (isOpen: boolean, onClose: () => void) => {
  const { activeConversation, setActiveConversation } = useChatStore();
  const { user } = useAuthStore();
  const { users, requestUser } = useUserStore();
  const router = useRouter();
  const queryClient = useQueryClient();

  const [allUsers, setAllUsers] = useState<User[]>([]);
  const [selectedUserIds, setSelectedUserIds] = useState<string[]>([]);
  const [groupName, setGroupName] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  useEffect(() => {
    if (isOpen) {
      setGroupName("");
      setSearchQuery("");
      setSelectedUserIds([]);
      setIsSubmitting(false);

      userApi
        .getUsers()
        .then((res) => setAllUsers(res.data || res))
        .catch(console.error);
    }
  }, [isOpen]);

  // Members from active conversation + current user
  const existingMembersMap = new Map<string, User>();

  if (user?.id) {
    existingMembersMap.set(user.id, {
      id: user.id,
      name: user.name || "Tôi",
      email: user.email || "",
      avatar_url: user?.avatar_url,
    });
  }

  if (activeConversation?.member_ids) {
    activeConversation.member_ids.forEach((m) => {
      const mId = m as string;
      if (mId && !existingMembersMap.has(mId)) {
        const memberUser = users[mId];
        if (!memberUser) {
            requestUser(mId);
        }
        existingMembersMap.set(mId, memberUser || { id: mId, name: 'Loading...', email: '' });
      }
    });
  }

  const existingMembers = Array.from(existingMembersMap.values());
  const existingMemberIds = new Set(existingMembersMap.keys());

  // Filter available users (not already in existing conversation)
  const availableUsers = allUsers.filter((u) => {
    const uId = u.id || u._id;
    return uId && !existingMemberIds.has(uId);
  });

  // Filter by search query
  const filteredUsers = availableUsers.filter((u) => {
    if (!searchQuery.trim()) return true;
    const q = searchQuery.toLowerCase();
    return (
      u.name?.toLowerCase().includes(q) ||
      u.email?.toLowerCase().includes(q)
    );
  });

  const toggleSelectUser = (userId: string) => {
    setSelectedUserIds((prev) =>
      prev.includes(userId)
        ? prev.filter((id) => id !== userId)
        : [...prev, userId]
    );
  };

  const handleCreateGroup = async () => {
    if (selectedUserIds.length === 0 || isSubmitting) return;

    try {
      setIsSubmitting(true);
      // 1. NẾU ĐANG LÀ NHÓM SẴN: THỰC HIỆN THÊM THÀNH VIÊN
      if (activeConversation?.type === "group" && activeConversation.id) {
        await conversationApi.addMembers(activeConversation.id, selectedUserIds);
        
        // Cập nhật lại cache để load danh sách mới nhất
        queryClient.invalidateQueries({ queryKey: ["conversations"] });
        
        onClose();
        return;
      }

      // 2. NẾU TỪ CHAT 1-1: TIẾN HÀNH TẠO NHÓM MỚI
      const allMemberIds = Array.from(
        new Set([...Array.from(existingMemberIds), ...selectedUserIds])
      );
      const defaultName =
        groupName.trim() ||
        `Nhóm ${existingMembers.map((m) => m.name).join(", ")}`;

      const res = await conversationApi.createGroup(defaultName, allMemberIds);
      const newGroup = res.data || res;

      setActiveConversation(newGroup);
      queryClient.invalidateQueries({ queryKey: ["conversations"] });

      onClose();
    } catch (error) {
      console.error("Lỗi tạo nhóm:", error);
    } finally {
      setIsSubmitting(false);
    }
  };

  return {
    groupName,
    setGroupName,
    searchQuery,
    setSearchQuery,
    existingMembers,
    filteredUsers,
    selectedUserIds,
    toggleSelectUser,
    handleCreateGroup,
    isSubmitting,
    isGroup: activeConversation?.type === "group"
  };
};
