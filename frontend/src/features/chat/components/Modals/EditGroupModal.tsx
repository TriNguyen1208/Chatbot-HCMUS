"use client";
import React, { useState, useRef, useEffect } from "react";
import { createPortal } from "react-dom";
import Image from "next/image";
import { Camera, Loader2, Save, X, Maximize, Users, Smile } from "lucide-react";
import data from '@emoji-mart/data';
import Picker from '@emoji-mart/react';
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

const COMMON_REACTIONS = ["👍", "❤️", "😆", "😮", "😢", "😡", "🔥", "🎉"];

export const EditGroupModal: React.FC<EditGroupModalProps> = ({
  isOpen,
  onClose,
  conversation,
  onSuccess,
}) => {
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

  if (!isOpen || !conversation || !mounted) return null;

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
        conversation.id || conversation._id,
        updatedData
      );
      onSuccess(res.data || res);
      onClose();
    } catch (error) {
      console.error("Failed to update conversation", error);
    } finally {
      setIsSaving(false);
    }
  };

  return createPortal(
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-md p-4 animate-in fade-in duration-200"
      onClick={onClose}
    >
      <div
        className="bg-surface/90 backdrop-blur-2xl rounded-[2rem] w-full max-w-[460px] max-h-[85vh] flex flex-col overflow-hidden shadow-2xl border border-glass-border animate-in zoom-in-95 duration-200"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between p-5 border-b border-glass-border bg-surface-solid/50">
          <div className="flex items-center gap-3">
            <div className="size-10 rounded-xl bg-brand-primary/10 border border-brand-primary/20 flex items-center justify-center text-brand-primary shrink-0 shadow-sm">
              {isGroup ? <Users className="size-5" /> : <Smile className="size-5" />}
            </div>
            <div className="flex flex-col">
              <h3 className="font-semibold text-base text-txt-primary leading-tight">
                {isGroup ? "Chỉnh sửa thông tin nhóm" : "Chỉnh sửa đoạn chat"}
              </h3>
              <p className="text-xs text-txt-extra mt-0.5">
                {isGroup
                  ? "Thay đổi tên, ảnh đại diện hoặc biểu tượng cảm xúc chính"
                  : "Tùy chỉnh biểu tượng cảm xúc chính của cuộc trò chuyện"}
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            disabled={isSaving}
            className="p-1.5 hover:bg-hover text-txt-extra hover:text-txt-primary rounded-xl transition-colors cursor-pointer disabled:opacity-50"
          >
            <X size={18} />
          </button>
        </div>

        {/* Form Body */}
        <form onSubmit={handleSave} className="flex flex-col flex-1 overflow-hidden">
          <div className="p-6 flex flex-col gap-6 overflow-y-auto">
            {/* Avatar Section for Group */}
            {isGroup && (
              <div className="flex flex-col items-center gap-3">
                <div
                  className="relative group cursor-pointer"
                  onClick={() => setShowImageModal(true)}
                >
                  <div
                    className={`w-28 h-28 rounded-full overflow-hidden border-4 border-glass-border shadow-lg relative ${
                      isUploading ? "opacity-50" : ""
                    }`}
                  >
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
                      <Loader2
                        className="animate-spin text-brand-primary bg-surface/80 rounded-full p-1"
                        size={28}
                      />
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
                  disabled={isUploading || isSaving}
                  className="text-xs font-medium px-3.5 py-2 bg-input-surface hover:bg-hover border border-glass-border rounded-xl text-txt-primary transition-colors flex items-center gap-2 shadow-sm cursor-pointer disabled:opacity-50"
                >
                  <Camera size={15} />
                  {isUploading ? "Đang tải lên..." : "Đổi ảnh đại diện"}
                </button>
              </div>
            )}

            {/* Name Field for Group */}
            {isGroup && (
              <div className="flex flex-col gap-2">
                <label className="text-xs font-semibold text-txt-secondary ml-1">
                  Tên nhóm
                </label>
                <input
                  type="text"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="Nhập tên nhóm"
                  required
                  className="w-full px-4 py-2.5 text-sm bg-input-surface border border-glass-border rounded-xl text-txt-primary focus:outline-none focus:ring-2 focus:ring-brand-primary/20 focus:border-brand-primary transition-all placeholder:text-txt-extra shadow-inner"
                />
              </div>
            )}

            {/* Primary Icon Section */}
            <div className="flex flex-col gap-3">
              <div className="flex items-center justify-between">
                <label className="text-xs font-semibold text-txt-secondary ml-1">
                  Biểu tượng cảm xúc chính (Primary Icon)
                </label>
                <span className="text-[11px] text-txt-extra">Mặc định: 👍</span>
              </div>

              {/* Selected Primary Icon Preview & Quick Selector */}
              <div
                ref={previewCardRef}
                onClick={() => setShowEmojiPicker(!showEmojiPicker)}
                className="flex items-center gap-3 p-3.5 bg-input-surface hover:bg-hover/60 border border-glass-border rounded-2xl cursor-pointer transition-colors"
              >
                <div
                  className="size-14 text-3xl rounded-2xl bg-surface-solid border-2 border-brand-primary/40 hover:border-brand-primary flex items-center justify-center shadow-md hover:scale-105 transition-all select-none shrink-0"
                  title="Bấm để chọn biểu tượng khác"
                >
                  <span>{primaryIcon}</span>
                </div>

                <div className="flex flex-col min-w-0 flex-1">
                  <span className="text-xs font-medium text-txt-primary">
                    Biểu tượng hiện tại: <span className="text-base">{primaryIcon}</span>
                  </span>
                  <span className="text-[11px] text-txt-extra mt-0.5">
                    Nhấp vào ô biểu tượng để mở bảng chọn emoji đầy đủ
                  </span>
                </div>
              </div>

              {/* Emoji-Mart Picker Rendered Underneath */}
              {showEmojiPicker && (
                <div 
                  ref={emojiPickerRef}
                  className="w-full flex justify-center py-1 animate-in fade-in zoom-in-95 duration-200 overflow-hidden rounded-2xl shadow-md border border-glass-border bg-surface-solid"
                >
                  <Picker
                    data={data}
                    onEmojiSelect={handleEmojiSelect}
                    theme="light"
                    previewPosition="none"
                    skinTonePosition="none"
                  />
                </div>
              )}

              {/* Quick Reactions Row */}
              <div className="flex flex-wrap items-center gap-2 mt-1">
                <span className="text-[11px] text-txt-extra mr-1 font-medium">Gợi ý nhanh:</span>
                {COMMON_REACTIONS.map((emoji) => (
                  <button
                    key={emoji}
                    type="button"
                    onClick={() => setPrimaryIcon(emoji)}
                    className={`size-9 text-lg rounded-xl flex items-center justify-center transition-all cursor-pointer select-none ${
                      primaryIcon === emoji
                        ? "bg-brand-primary/15 border-2 border-brand-primary scale-110 shadow-sm"
                        : "bg-surface-solid/60 border border-glass-border hover:bg-hover hover:scale-110"
                    }`}
                  >
                    {emoji}
                  </button>
                ))}
              </div>
            </div>
          </div>

          {/* Footer */}
          <div className="flex items-center justify-end gap-2.5 p-4 border-t border-glass-border bg-surface-solid/50">
            <button
              type="button"
              onClick={onClose}
              disabled={isSaving || isUploading}
              className="px-4 py-2.5 text-sm font-medium text-txt-primary hover:bg-hover rounded-xl transition-colors cursor-pointer disabled:opacity-50"
            >
              Hủy
            </button>
            <button
              type="submit"
              disabled={isSaving || isUploading || (isGroup && !name.trim())}
              className="px-5 py-2.5 text-sm font-medium text-white bg-gradient-to-r from-brand-primary to-brand-primary/90 hover:opacity-95 rounded-xl disabled:opacity-50 disabled:shadow-none disabled:cursor-not-allowed transition-all shadow-md hover:shadow-lg hover:-translate-y-0.5 cursor-pointer flex items-center gap-2"
            >
              {isSaving ? (
                <>
                  <div className="size-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                  <span>Đang lưu...</span>
                </>
              ) : (
                <>
                  <Save size={16} />
                  <span>Lưu thay đổi</span>
                </>
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
    </div>,
    document.body
  );
};

export const EditConversationModal = EditGroupModal;
