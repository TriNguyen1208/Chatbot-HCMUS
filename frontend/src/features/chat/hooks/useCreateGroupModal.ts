import { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import { useQueryClient } from "@tanstack/react-query";
import { userApi, User } from "@/features/chat/api/user.api";
import { conversationApi } from "@/features/chat/api/conversation.api";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";

export const useCreateGroupModal = (isOpen: boolean, onClose: () => void) => {
  const { activeConversation, setActiveConversation } = useChatStore();
  const { user } = useAuthStore();
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
      avatar_url: (user as any)?.avatar_url,
    });
  }

  if (activeConversation?.members) {
    activeConversation.members.forEach((m) => {
      const mId = m.id || (m as any)._id;
      if (mId && !existingMembersMap.has(mId)) {
        existingMembersMap.set(mId, m);
      }
    });
  }

  const existingMembers = Array.from(existingMembersMap.values());
  const existingMemberIds = new Set(existingMembersMap.keys());

  // Filter available users (not already in existing conversation)
  const availableUsers = allUsers.filter((u) => {
    const uId = u.id || (u as any)._id;
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
      const allMemberIds = Array.from(
        new Set([...Array.from(existingMemberIds), ...selectedUserIds])
      );

      const defaultName =
        groupName.trim() ||
        `Nhóm ${existingMembers.map((m) => m.name).join(", ")}`;

      const res = await conversationApi.createGroup(defaultName, allMemberIds);
      const newGroup = res.data || res;

      // Update active conversation & invalidate query cache
      setActiveConversation(newGroup);
      queryClient.invalidateQueries({ queryKey: ["conversations"] });

      const newId = newGroup._id || newGroup.id;
      router.push(`/group-chat?conversation_id=${newId}`);

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
  };
};
