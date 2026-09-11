"use client";
import { format } from "date-fns";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useUserStore } from "@/features/chat/stores/userStore";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useModalStore } from "@/features/chat/stores/modalStore";
import { messageApi } from "@/features/chat/api/message.api";
import type { Message } from "@/types";
import { useState, useRef, useEffect } from "react";
import { find } from "linkifyjs";

export interface UseMessageItemOptions {
  isLastMessage?: boolean;
  watermarks?: { type: 'delivered' | 'read'; userId: string }[];
}

export const useMessageItem = (message: Message, options?: UseMessageItemOptions) => {
  const isLastMessage = options?.isLastMessage ?? false;
  const watermarks = options?.watermarks;
  
  const { user } = useAuthStore();
  const requestUser = useUserStore(state => state.requestUser);
  const users = useUserStore(state => state.users);
  const senderUser = users[message.sender_id || ''];

  useEffect(() => {
    if (message.sender_id && !senderUser) {
      requestUser(message.sender_id);
    }
  }, [message.sender_id, senderUser, requestUser]);

  useEffect(() => {
    if (watermarks) {
      watermarks.forEach(w => {
        if (w.userId && !users[w.userId]) {
          requestUser(w.userId);
        }
      });
    }
  }, [watermarks, users, requestUser]);

  const senderId = message.sender_id;
  const isMe = senderId === user?.id;
  const isSystem = message.type === 'system' || senderId === 'system';

  const timeDisplay = message.created_at ? format(new Date(message.created_at), "h:mm a") : "";

  const [showMenu, setShowMenu] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);

  const [showReactMenu, setShowReactMenu] = useState(false);
  const reactMenuRef = useRef<HTMLDivElement>(null);

  const [showReactionList, setShowReactionList] = useState(false);
  const [showImageModal, setShowImageModal] = useState(false);
  const [showDetails, setShowDetails] = useState(false);
  const [showEditHistory, setShowEditHistory] = useState(false);

  const shouldShowDetails = isLastMessage || showDetails;

  const setEditingMessage = useChatStore(state => state.setEditingMessage);

  const canEdit = Boolean(
    isMe && 
    message.status !== 'recalled' && 
    message.type === 'text' && 
    (Date.now() - new Date(message.created_at || Date.now()).getTime() <= 60 * 60 * 1000)
  );

  const handleEdit = () => {
    setShowMenu(false);
    setEditingMessage(message);
  };

  const detectedLinks = message.content && message.status !== 'recalled' ? find(message.content, 'url') : [];
  const previewUrl = detectedLinks.length > 0 ? detectedLinks[0].href : null;

  const handleOpenUserProfile = () => {
    if (message.sender_id) {
      useModalStore.getState().openUserProfileModal(message.sender_id);
    }
  };

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
        setShowMenu(false);
      }
      if (reactMenuRef.current && !reactMenuRef.current.contains(event.target as Node)) {
        setShowReactMenu(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const handleRecall = async () => {
    try {
      if (message.id) {
        await messageApi.recallMessage(message.id);
      }
      setShowMenu(false);
    } catch (error) {
      console.error("Failed to recall message", error);
    }
  };

  const handleForward = () => {
    useModalStore.getState().openForwardModal(message);
    setShowMenu(false);
  };

  const handleReact = async (emoji: string) => {
    setShowReactMenu(false);
    try {
      if (message.id) {
        await messageApi.toggleReaction(message.id, emoji);
      }
    } catch (error) {
      console.error("Failed to toggle reaction", error);
    }
  };

  return {
    isMe,
    isSystem,
    senderUser,
    timeDisplay,
    showMenu,
    setShowMenu,
    menuRef,
    showReactMenu,
    setShowReactMenu,
    reactMenuRef,
    showReactionList,
    setShowReactionList,
    showImageModal,
    setShowImageModal,
    showDetails,
    setShowDetails,
    shouldShowDetails,
    showEditHistory,
    setShowEditHistory,
    canEdit,
    handleEdit,
    previewUrl,
    handleRecall,
    handleForward,
    handleReact,
    handleOpenUserProfile,
    users
  };
};
