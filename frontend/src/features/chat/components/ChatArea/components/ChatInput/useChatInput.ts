"use client";
import { useState, useRef } from "react";
import { messageApi } from "@/features/chat/api/message.api";
import { mediaApi } from "@/features/chat/api/media.api";
import { uploadService } from "@/shared/services/upload.service";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useUserStore } from "@/features/chat/stores/userStore";
import { conversationApi } from "@/features/chat/api/conversation.api";
import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useCallback } from "react";
import { useSocketContext } from "@/providers/SocketProvider";
import { format } from "date-fns";

export const useChatInput = () => {
  const [content, setContent] = useState("");
  
  const fileInputRef = useRef<HTMLInputElement>(null);
  
  const [uploadedMedia, setUploadedMedia] = useState<{ type: 'image' | 'video', url?: string, uid?: string } | null>(null);
  const [previewUrl, setPreviewUrl] = useState<string | null>(null);
  
  const [isPreviewLoading, setIsPreviewLoading] = useState(false);
  const [isUploading, setIsUploading] = useState(false);

  const activeConversation = useChatStore(state => state.activeConversation);
  const { user } = useAuthStore();
  const { socket } = useSocketContext();
  const typingTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  const editingMessage = useChatStore(state => state.editingMessage);
  const setEditingMessage = useChatStore(state => state.setEditingMessage);

  useEffect(() => {
    if (editingMessage) {
      setContent(editingMessage.content || "");
    }
  }, [editingMessage]);

  const emitTyping = useCallback(() => {
    if (!socket || !activeConversation || !user) return;
    const convId = activeConversation.id || activeConversation.id;
    if (!convId) return;

    const receiverIds = activeConversation.member_ids?.filter((id: string) => id !== user.id) || [];
    socket.emit('typing', { conversationId: convId, name: user.name, receiverIds });

    if (typingTimeoutRef.current) {
      clearTimeout(typingTimeoutRef.current);
    }
    typingTimeoutRef.current = setTimeout(() => {
      socket.emit('stop_typing', { conversationId: convId, receiverIds });
    }, 2000);
  }, [socket, activeConversation, user]);

  const emitStopTyping = useCallback(() => {
    if (!socket || !activeConversation || !user) return;
    const convId = activeConversation.id || activeConversation.id;
    if (!convId) return;
    
    const receiverIds = activeConversation.member_ids?.filter((id: string) => id !== user.id) || [];
    socket.emit('stop_typing', { conversationId: convId, receiverIds });
    
    if (typingTimeoutRef.current) {
      clearTimeout(typingTimeoutRef.current);
    }
  }, [socket, activeConversation, user]);

  const handleContentChange = (val: string) => {
    setContent(val);
    if (val.trim()) {
      emitTyping();
    } else {
      emitStopTyping();
    }
  };

  const uploadVideoMultipart = async (file: File) => {
    return uploadService.uploadVideoMultipart(file);
  };

  const handleSend = async () => {
    if ((!content.trim() && !uploadedMedia) || !activeConversation || isUploading || isPreviewLoading) return;

    if (editingMessage) {
      const currentContent = content;
      setContent("");
      setIsUploading(true);
      try {
        await messageApi.editMessage(editingMessage.id as string || (editingMessage as any)._id as string, currentContent);
        setEditingMessage(null);
      } catch (error) {
        console.error("Failed to edit message", error);
      } finally {
        setIsUploading(false);
      }
      return;
    }

    const currentContent = content;
    const currentMedia = uploadedMedia;
    
    setContent("");
    emitStopTyping();
    setUploadedMedia(null);
    setPreviewUrl(null);
    setIsUploading(true);

    try {
      const payload: Record<string, unknown> = { type: 'text' };
      if (currentContent.trim()) payload.content = currentContent;

      if (currentMedia) {
        if (currentMedia.type === 'image') {
          payload.type = 'image';
          payload.image = { url: currentMedia.url };
        } else if (currentMedia.type === 'video') {
          payload.type = 'video';
          payload.video = { file_key: currentMedia.uid, url: currentMedia.url };
        }
      }

      const convId = activeConversation.id || activeConversation.id;
      
      if (!convId && activeConversation.type === 'utu') {
        const members = activeConversation.member_ids || [];
        const receiverId = activeConversation.receiver_id || members.find((m: string) => m !== user?.id) || members[0];
        payload.receiver_id = receiverId; // Báo cho server biết người nhận là ai để server tự gom box chat (hoặc tạo box mới)
      } else {
        payload.conversation_id = convId; // Nhắn vào box chat cụ thể đã có sẵn
      }

      await messageApi.sendMessage(payload);

    } catch (error) {
      console.error("Failed to send message", error);
    } finally {
      setIsUploading(false);
    }
  };

  const cancelEdit = () => {
    setEditingMessage(null);
    setContent("");
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') handleSend();
  };

  const handleFileClick = () => {
    fileInputRef.current?.click();
  };

  const handleFileChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    if (fileInputRef.current) fileInputRef.current.value = "";

    setIsPreviewLoading(true);
    setPreviewUrl(null);
    setUploadedMedia(null);

    try {
      if (file.type.startsWith('image/')) {
        const blobUrl = URL.createObjectURL(file);
        setPreviewUrl(blobUrl);
        const res = await mediaApi.uploadImage(file);
        const url = (res as any)?.resource_url || res.url;
        if (url) {
          setPreviewUrl(url); 
          setUploadedMedia({ type: 'image', url }); 
        }
      } else if (file.type.startsWith('video/')) {
        const blobUrl = URL.createObjectURL(file);
        setPreviewUrl(blobUrl);
        const { fileKey, resourceUrl } = await uploadVideoMultipart(file);
        setUploadedMedia({ type: 'video', uid: fileKey, url: resourceUrl });
      }
    } catch (error) {
      console.error("Failed to upload media", error);
    } finally {
      setIsPreviewLoading(false);
    }
  };

  const removeSelectedFile = () => {
    setUploadedMedia(null);
    setPreviewUrl(null);
  };

  const [showEmojiPicker, setShowEmojiPicker] = useState(false);
  const emojiPickerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (emojiPickerRef.current && !emojiPickerRef.current.contains(event.target as Node)) {
        setShowEmojiPicker(false);
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
    };
  }, []);

  const handleEmojiSelect = (emoji: { native: string }) => {
    setContent((prev) => prev + emoji.native);
  };

  const handleSendPrimaryIcon = async () => {
    if (!activeConversation || isUploading || isPreviewLoading) return;

    const primaryIcon = activeConversation.primary_icon || '👍';
    setIsUploading(true);

    try {
      const payload: Record<string, unknown> = {
        type: 'text',
        content: primaryIcon
      };

      const convId = activeConversation.id;
      if (!convId && activeConversation.type === 'utu') {
        const members = activeConversation.member_ids || [];
        const receiverId = activeConversation.receiver_id || members.find((m: string) => m !== user?.id) || members[0];
        payload.receiver_id = receiverId;
      } else {
        payload.conversation_id = convId;
      }

      await messageApi.sendMessage(payload);
    } catch (error) {
      console.error("Failed to send primary icon", error);
    } finally {
      setIsUploading(false);
    }
  };

  const users = useUserStore((state) => state.users);
  const queryClient = useQueryClient();
  const [isUnblocking, setIsUnblocking] = useState(false);

  useEffect(() => {
    if (activeConversation?.block?.block_by) {
      const bId = activeConversation.block.block_by;
      if (!users[bId]) {
        useUserStore.getState().requestUser(bId);
      }
    }
  }, [activeConversation?.block?.block_by, users]);

  const isBlocked = Boolean(activeConversation?.type === "utu" && activeConversation?.block);
  const isBlocker = Boolean(isBlocked && activeConversation?.block?.block_by === user?.id);
  const blockerUser = activeConversation?.block?.block_by ? users[activeConversation.block.block_by] : null;
  const blockerName = blockerUser?.name || "Người này";

  let blockTimeDisplay = "";
  if (isBlocked && activeConversation?.block?.block_at) {
    try {
      blockTimeDisplay = format(new Date(activeConversation.block.block_at), "HH:mm - dd/MM/yyyy");
    } catch {
      blockTimeDisplay = String(activeConversation.block.block_at);
    }
  }

  const handleUnblock = async () => {
    if (!activeConversation?.id || isUnblocking) return;
    try {
      setIsUnblocking(true);
      const res = await conversationApi.unblockConversation(activeConversation.id as string);
      const updatedConv = (res as any).data || res;
      useChatStore.getState().setActiveConversation(updatedConv);
      queryClient.invalidateQueries({ queryKey: ["conversations"] });
    } catch (error: any) {
      console.error("Lỗi bỏ chặn:", error);
      alert(error?.response?.data?.message || error?.message || "Không thể bỏ chặn người dùng");
    } finally {
      setIsUnblocking(false);
    }
  };

  return {
    content,
    setContent: handleContentChange,
    fileInputRef,
    uploadedMedia,
    previewUrl,
    isPreviewLoading,
    isUploading,
    handleSend,
    handleSendPrimaryIcon,
    handleKeyDown,
    handleFileClick,
    handleFileChange,
    removeSelectedFile,
    emojiPickerRef,
    showEmojiPicker,
    setShowEmojiPicker,
    handleEmojiSelect,
    editingMessage,
    cancelEdit,
    isBlocked,
    isBlocker,
    blockerName,
    blockTimeDisplay,
    handleUnblock,
    isUnblocking,
    primaryIcon: activeConversation?.primary_icon || '👍',
  };
};
