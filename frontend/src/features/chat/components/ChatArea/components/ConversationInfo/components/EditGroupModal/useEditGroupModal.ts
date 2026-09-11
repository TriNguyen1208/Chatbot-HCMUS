"use client";

import { useState, useRef, useEffect } from "react";
import { conversationApi } from "@/features/chat/api/conversation.api";
import { mediaApi } from "@/features/chat/api/media.api";

interface UseEditGroupModalProps {
  isOpen: boolean;
  onClose: () => void;
  conversation: any;
  onSuccess: (updatedConv: any) => void;
}

export const useEditGroupModal = ({
  isOpen,
  onClose,
  conversation,
  onSuccess,
}: UseEditGroupModalProps) => {
  const [mounted, setMounted] = useState(false);
  const [name, setName] = useState("");
  const [avatarUrl, setAvatarUrl] = useState("");
  const [primaryIcon, setPrimaryIcon] = useState("👍");
  const [showEmojiPicker, setShowEmojiPicker] = useState(false);
  const [isUploading, setIsUploading] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [showImageModal, setShowImageModal] = useState(false);

  const fileInputRef = useRef<HTMLInputElement>(null);
  const emojiPickerRef = useRef<HTMLDivElement>(null);
  const previewCardRef = useRef<HTMLDivElement>(null);

  const isGroup = conversation?.type === "group";

  useEffect(() => {
    setMounted(true);
  }, []);

  useEffect(() => {
    if (isOpen && conversation) {
      setName(conversation.name || "");
      setAvatarUrl(conversation.avatar_url || "");
      setPrimaryIcon(conversation.primary_icon || "👍");
      setShowEmojiPicker(false);
    }
  }, [isOpen, conversation]);

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (
        emojiPickerRef.current &&
        !emojiPickerRef.current.contains(e.target as Node) &&
        !previewCardRef.current?.contains(e.target as Node)
      ) {
        setShowEmojiPicker(false);
      }
    };

    if (showEmojiPicker) {
      document.addEventListener("mousedown", handleClickOutside);
    }
    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
    };
  }, [showEmojiPicker]);

  const handleAvatarClick = () => {
    if (fileInputRef.current) {
      fileInputRef.current.click();
    }
  };

  const handleFileChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    e.target.value = "";

    if (!file.type.startsWith("image/")) {
      return;
    }

    try {
      setIsUploading(true);
      const res = await mediaApi.uploadImage(file);
      const url = (res as any)?.resource_url || res.url;
      if (url) {
        setAvatarUrl(url);
      }
    } catch (error) {
      console.error("Failed to upload group avatar", error);
    } finally {
      setIsUploading(false);
    }
  };

  const handleEmojiSelect = (emoji: { native: string }) => {
    setPrimaryIcon(emoji.native);
    setShowEmojiPicker(false);
  };

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault();
    if (isGroup && !name.trim()) return;

    try {
      setIsSaving(true);
      const updatedData: { name?: string; avatar_url?: string; primary_icon?: string } = {
        primary_icon: primaryIcon,
      };

      if (isGroup) {
        updatedData.name = name.trim();
        updatedData.avatar_url = avatarUrl;
      }

      const res = await conversationApi.updateConversation(
        conversation.id || (conversation as any)._id,
        updatedData
      );
      onSuccess(res);
      onClose();
    } catch (error) {
      console.error("Failed to update conversation", error);
    } finally {
      setIsSaving(false);
    }
  };

  return {
    mounted,
    name,
    setName,
    avatarUrl,
    primaryIcon,
    setPrimaryIcon,
    showEmojiPicker,
    setShowEmojiPicker,
    isUploading,
    isSaving,
    showImageModal,
    setShowImageModal,
    fileInputRef,
    emojiPickerRef,
    previewCardRef,
    isGroup,
    handleAvatarClick,
    handleFileChange,
    handleEmojiSelect,
    handleSave,
  };
};
