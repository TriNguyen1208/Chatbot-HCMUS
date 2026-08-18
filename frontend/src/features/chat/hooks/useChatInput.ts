import { useState, useRef } from "react";
import { messageApi } from "../api/message.api";
import { mediaApi } from "../api/media.api";
import { useChatStore } from "../stores/chatStore";
import { useAuthStore } from "@/features/auth/stores/authStore";

export const useChatInput = () => {
  const [content, setContent] = useState("");
  
  const fileInputRef = useRef<HTMLInputElement>(null);
  
  const [uploadedMedia, setUploadedMedia] = useState<{ type: 'image' | 'video', url?: string, uid?: string } | null>(null);
  const [previewUrl, setPreviewUrl] = useState<string | null>(null);
  
  const [isPreviewLoading, setIsPreviewLoading] = useState(false);
  const [isUploading, setIsUploading] = useState(false);

  const activeConversation = useChatStore(state => state.activeConversation);
  const { user } = useAuthStore();

  const uploadVideoMultipart = async (file: File) => {
    const CHUNK_SIZE = 5 * 1024 * 1024; 
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    
    const initRes = await mediaApi.initMultipartUpload(file.name, file.type);
    const { uploadId, fileKey } = initRes.data;

    const partNumbers = Array.from({ length: totalChunks }, (_, i) => i + 1);
    const urlRes = await mediaApi.getPresignedUrlsForMultipart(fileKey, uploadId, partNumbers);
    const presignedUrls = urlRes.data.urls;

    const uploadedParts: { ETag: string; PartNumber: number }[] = [];

    const uploadPromises = partNumbers.map(async (partNumber, index) => {
      const start = (partNumber - 1) * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);
      const presignedUrl = presignedUrls[index];

      const uploadRes = await fetch(presignedUrl, {
        method: "PUT",
        body: chunk,
      });

      const eTag = uploadRes.headers.get("ETag")?.replace(/"/g, "") || "";
      uploadedParts.push({ ETag: eTag, PartNumber: partNumber });
    });

    await Promise.all(uploadPromises);

    const completeRes = await mediaApi.completeMultipartUpload(fileKey, uploadId, uploadedParts);
    const resourceUrl = completeRes?.data?.resource_url || completeRes?.resource_url;

    return { fileKey, resourceUrl };
  };

  const handleSend = async () => {
    if ((!content.trim() && !uploadedMedia) || !activeConversation || isUploading || isPreviewLoading) return;

    const currentContent = content;
    const currentMedia = uploadedMedia;
    
    setContent("");
    setUploadedMedia(null);
    setPreviewUrl(null);
    setIsUploading(true);

    try {
      const payload: any = { type: 'text' };
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

      const convId = activeConversation._id || (activeConversation as any).id;
      
      if (!convId && activeConversation.type === 'utu') {
        const members = activeConversation.members || [];
        const receiverId = (activeConversation as any).receiver_id || members.find((m: any) => m.id !== user?.id)?.id || members[0]?.id;
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
        const url = res.data?.resource_url;
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

  return {
    content,
    setContent,
    fileInputRef,
    uploadedMedia,
    previewUrl,
    isPreviewLoading,
    isUploading,
    handleSend,
    handleKeyDown,
    handleFileClick,
    handleFileChange,
    removeSelectedFile
  };
};
