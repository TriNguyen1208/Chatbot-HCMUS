import React, { useState, useRef, useEffect } from "react";
import Image from "next/image";
import { Camera, Loader2, Save, X, Maximize } from "lucide-react";
import { conversationApi } from "@/features/chat/api/conversation.api";
import { mediaApi } from "@/features/chat/api/media.api";
import { DEFAULT_AVATAR } from "@/utils/constants";
import MediaViewerModal from "./MediaViewerModal";

interface EditGroupModalProps {
  isOpen: boolean;
  onClose: () => void;
  conversation: any;
  onSuccess: (updatedConv: any) => void;
}

export const EditGroupModal: React.FC<EditGroupModalProps> = ({
  isOpen,
  onClose,
  conversation,
  onSuccess
}) => {
  const [name, setName] = useState("");
  const [avatarUrl, setAvatarUrl] = useState("");
  const [isUploading, setIsUploading] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [showImageModal, setShowImageModal] = useState(false);

  const fileInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (isOpen && conversation) {
      setName(conversation.name || "");
      setAvatarUrl(conversation.avatar_url || "");
    }
  }, [isOpen, conversation]);

  if (!isOpen || !conversation) return null;

  const handleAvatarClick = () => {
    if (fileInputRef.current) {
      fileInputRef.current.click();
    }
  };

  const handleFileChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    
    e.target.value = '';

    if (!file.type.startsWith("image/")) {
      return;
    }

    try {
      setIsUploading(true);
      const res = await mediaApi.uploadImage(file);
      const url = res.data?.resource_url || res.data?.url; 
      if (url) {
        setAvatarUrl(url);
      }
    } catch (error) {
      console.error("Failed to upload group avatar", error);
    } finally {
      setIsUploading(false);
    }
  };

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!name.trim()) return;

    try {
      setIsSaving(true);
      const updatedData = {
        name: name.trim(),
        avatar_url: avatarUrl
      };

      const res = await conversationApi.updateConversation(conversation.id || conversation._id, updatedData);
      onSuccess(res.data || res);
      onClose();
    } catch (error) {
      console.error("Failed to update group", error);
    } finally {
      setIsSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4 animate-in fade-in duration-200" onClick={onClose}>
      <div 
        className="relative w-full max-w-lg bg-surface-solid border border-glass-border rounded-2xl p-6 shadow-2xl flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-xl font-bold text-text-primary">Chỉnh sửa thông tin nhóm</h2>
          <button 
            onClick={onClose}
            className="p-2 rounded-full hover:bg-glass cursor-pointer text-gray-500 hover:text-text-primary transition-colors"
          >
            <X size={20} />
          </button>
        </div>

        <form onSubmit={handleSave} className="flex flex-col gap-6">
          {/* Avatar Section */}
          <div className="flex flex-col items-center gap-4">
            <div className="relative group cursor-pointer" onClick={() => setShowImageModal(true)}>
              <div className={`w-28 h-28 rounded-full overflow-hidden border-4 border-glass-border shadow-lg relative ${isUploading ? 'opacity-50' : ''}`}>
                <Image
                  src={avatarUrl || DEFAULT_AVATAR}
                  alt={name || "Group Avatar"}
                  fill
                  className="object-cover"
                />
                <div className="absolute inset-0 bg-black/40 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity duration-300">
                  <Maximize className="text-white" size={24} />
                </div>
              </div>
              
              {isUploading && (
                <div className="absolute inset-0 flex items-center justify-center">
                  <Loader2 className="animate-spin text-brand-primary bg-surface/80 rounded-full p-1" size={28} />
                </div>
              )}
            </div>

            <input 
              type="file" 
              ref={fileInputRef} 
              onChange={handleFileChange} 
              accept="image/*" 
              className="hidden" 
            />
            
            <button 
              type="button"
              onClick={handleAvatarClick}
              disabled={isUploading}
              className="text-sm px-4 py-2 bg-glass hover:bg-glass-panel border border-glass-border rounded-xl text-text-primary transition-colors flex items-center gap-2"
            >
              <Camera size={16} />
              {isUploading ? 'Đang tải lên...' : 'Đổi ảnh đại diện'}
            </button>
          </div>

          {/* Name Field */}
          <div className="flex flex-col gap-2">
            <label className="text-sm font-medium text-text-secondary ml-1">Tên nhóm</label>
            <input 
              type="text" 
              value={name} 
              onChange={(e) => setName(e.target.value)}
              placeholder="Nhập tên nhóm"
              required
              className="w-full px-4 py-3 bg-glass border border-glass-border rounded-xl text-text-primary focus:outline-none focus:ring-2 focus:ring-brand-primary/50 transition-all placeholder:text-text-secondary/50"
            />
          </div>

          {/* Actions */}
          <div className="flex justify-end gap-3 pt-4 border-t border-glass-border">
            <button 
              type="button"
              onClick={onClose}
              disabled={isSaving || isUploading}
              className="px-5 py-2.5 text-sm font-medium text-text-secondary bg-transparent hover:bg-glass rounded-xl transition-colors disabled:opacity-50"
            >
              Hủy
            </button>
            <button 
              type="submit"
              disabled={isSaving || isUploading || !name.trim()}
              className="flex items-center gap-2 px-5 py-2.5 text-sm font-medium text-white bg-brand-primary hover:bg-brand-primary/90 rounded-xl transition-colors shadow-lg disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isSaving ? (
                <><Loader2 size={18} className="animate-spin" /> Đang lưu...</>
              ) : (
                <><Save size={18} /> Lưu thay đổi</>
              )}
            </button>
          </div>
        </form>
      </div>

      <MediaViewerModal 
        isOpen={showImageModal}
        onClose={() => setShowImageModal(false)}
        mediaType="image"
        mediaUrl={avatarUrl || DEFAULT_AVATAR}
      />
    </div>
  );
};
