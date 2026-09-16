"use client";
import { useState, useRef, useCallback, useEffect } from "react";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useSearchStore } from "@/features/chat/stores/searchStore";
import { useChatHeader } from "@/features/chat/components/ChatArea/components/ChatHeader/useChatHeader";
import { useUserStore } from "@/features/chat/stores/userStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useModalStore } from "@/features/chat/stores/modalStore";
import { messageApi } from "@/features/chat/api/message.api";
import { conversationApi } from "@/features/chat/api/conversation.api";
import { SearchResult } from "@/features/chat/api/search.api";

const MIN_WIDTH = 250;
const MAX_WIDTH = 500;
const DEFAULT_WIDTH = 320;

export const useConversationInfo = () => {
  const showInfoPanel = useChatStore((state) => state.showInfoPanel);
  const toggleInfoPanel = useChatStore((state) => state.toggleInfoPanel);
  const setTargetMessageId = useSearchStore((state) => state.setTargetMessageId);
  const { user } = useAuthStore();
  const users = useUserStore((state) => state.users);

  const {
    setCreateGroupOpen,
    setAssignAdminModalOpen,
    setKickModalOpen,
    openUserProfileModal
  } = useModalStore();

  const {
    activeConversation,
    displayName,
    displayAvatar,
    otherMember,
    isOnline,
    handleLeaveGroup,
    isAdmin,
  } = useChatHeader();

  const [panelWidth, setPanelWidth] = useState(DEFAULT_WIDTH);
  const isResizing = useRef(false);
  
  const [isMembersExpanded, setIsMembersExpanded] = useState(true);
  const [isMediaExpanded, setIsMediaExpanded] = useState(true);

  // Media Mode State
  const [mediaMode, setMediaMode] = useState<'image' | 'video' | null>(null);
  const [mediaItems, setMediaItems] = useState<any[]>([]);
  const [isLoadingMedia, setIsLoadingMedia] = useState(false);
  const [selectedMedia, setSelectedMedia] = useState<{ url: string, type: 'image' | 'video' } | null>(null);

  const [showEditGroupModal, setShowEditGroupModal] = useState(false);
  const [showBlockModal, setShowBlockModal] = useState(false);
  const [showDisbandModal, setShowDisbandModal] = useState(false);
  const [isBlocking, setIsBlocking] = useState(false);
  const [isDisbanding, setIsDisbanding] = useState(false);

  const handleDisbandGroup = async () => {
    if (!activeConversation?.id || isDisbanding) return;

    try {
      setIsDisbanding(true);
      const convId = activeConversation.id as string;
      await conversationApi.disbandGroup(convId);

      setShowDisbandModal(false);
    } catch (error: unknown) {
      console.error("Lỗi khi giải tán nhóm:", error);
      alert((error as Error)?.message || "Không thể giải tán nhóm");
    } finally {
      setIsDisbanding(false);
    }
  };

  const handleBlockUser = async () => {
    if (!activeConversation?.id || isBlocking) return;

    try {
      setIsBlocking(true);
      const res = await conversationApi.blockConversation(activeConversation.id as string);
      const updatedConv = (res as any)?.data || res;
      useChatStore.getState().setActiveConversation(updatedConv);
      setShowBlockModal(false);
    } catch (error: unknown) {
      console.error("Lỗi khi chặn người dùng:", error);
      alert((error as Error)?.message || "Không thể chặn người dùng");
    } finally {
      setIsBlocking(false);
    }
  };

  const handleUnblockUser = async () => {
    if (!activeConversation?.id || isBlocking) return;
    try {
      setIsBlocking(true);
      const res = await conversationApi.unblockConversation(activeConversation.id as string);
      const updatedConv = (res as any)?.data || res;
      useChatStore.getState().setActiveConversation(updatedConv);
    } catch (error: unknown) {
      console.error("Lỗi khi bỏ chặn người dùng:", error);
      alert((error as Error)?.message || "Không thể bỏ chặn người dùng");
    } finally {
      setIsBlocking(false);
    }
  };

  useEffect(() => {
    if (!mediaMode || !activeConversation) {
      setMediaItems([]);
      return;
    }

    const fetchMedia = async () => {
      setIsLoadingMedia(true);
      try {
        const res = await messageApi.getMessages(activeConversation.id as string, 50, undefined, undefined, mediaMode);
        const resultsArray = Array.isArray((res as any)?.data) ? (res as any).data : res;
        setMediaItems(Array.isArray(resultsArray) ? resultsArray : []);
      } catch (error) {
        console.error("Fetch media failed:", error);
      } finally {
        setIsLoadingMedia(false);
      }
    };
    fetchMedia();
  }, [mediaMode, activeConversation?.id]);

  // Search Mode State
  const [isSearchMode, setIsSearchMode] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [isSearching, setIsSearching] = useState(false);
  const debounceTimeout = useRef<NodeJS.Timeout | null>(null);

  useEffect(() => {
    if (!isSearchMode || !activeConversation) {
      setSearchQuery("");
      setSearchResults([]);
      return;
    }
    if (!searchQuery.trim()) {
      setSearchResults([]);
      return;
    }

    setIsSearching(true);
    if (debounceTimeout.current) clearTimeout(debounceTimeout.current);

    debounceTimeout.current = setTimeout(async () => {
      try {
        const res = await messageApi.getMessages(activeConversation.id as string, 20, undefined, searchQuery);
        const resultsArray = Array.isArray((res as any)?.data) ? (res as any).data : res;
        setSearchResults(Array.isArray(resultsArray) ? (resultsArray as unknown as SearchResult[]) : []);
      } catch (error) {
        console.error("Search failed:", error);
      } finally {
        setIsSearching(false);
      }
    }, 500);

    return () => {
      if (debounceTimeout.current) clearTimeout(debounceTimeout.current);
    };
  }, [searchQuery, isSearchMode, activeConversation?.id]);

  // Resize logic
  const handleMouseDown = (_e: React.MouseEvent) => {
    isResizing.current = true;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
  };

  const handleMouseMove = useCallback((e: MouseEvent) => {
    if (!isResizing.current) return;
    const newWidth = window.innerWidth - e.clientX;
    
    if (newWidth >= MIN_WIDTH && newWidth <= MAX_WIDTH) {
      setPanelWidth(newWidth);
    } else if (newWidth < MIN_WIDTH) {
      setPanelWidth(MIN_WIDTH);
    } else if (newWidth > MAX_WIDTH) {
      setPanelWidth(MAX_WIDTH);
    }
  }, []);

  const handleMouseUp = useCallback(() => {
    isResizing.current = false;
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
  }, []);

  useEffect(() => {
    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);
    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [handleMouseMove, handleMouseUp]);

  return {
    showInfoPanel,
    toggleInfoPanel,
    panelWidth,
    handleMouseDown,
    activeConversation,
    displayName,
    displayAvatar,
    otherMember,
    isOnline,
    isAdmin,
    user,
    users,
    setTargetMessageId,
    setCreateGroupOpen,
    setAssignAdminModalOpen,
    setKickModalOpen,
    openUserProfileModal,
    // Search
    isSearchMode,
    setIsSearchMode,
    searchQuery,
    setSearchQuery,
    searchResults,
    isSearching,
    // Media
    mediaMode,
    setMediaMode,
    mediaItems,
    isLoadingMedia,
    selectedMedia,
    setSelectedMedia,
    isMediaExpanded,
    setIsMediaExpanded,
    // Members
    isMembersExpanded,
    setIsMembersExpanded,
    // Danger & Modals
    showEditGroupModal,
    setShowEditGroupModal,
    showBlockModal,
    setShowBlockModal,
    showDisbandModal,
    setShowDisbandModal,
    isBlocking,
    isDisbanding,
    handleLeaveGroup,
    handleDisbandGroup,
    handleBlockUser,
    handleUnblockUser,
  };
};
