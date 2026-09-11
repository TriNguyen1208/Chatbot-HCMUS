"use client";
import React from "react";
import { createPortal } from "react-dom";
import { X, Ban, ShieldAlert } from "lucide-react";
import Image from "next/image";
import { DEFAULT_AVATAR } from "@/config/constants";
import { useBlockUserModal } from "./useBlockUserModal";

export interface BlockUserModalProps {
  isOpen: boolean;
  onClose: () => void;
  onConfirm: () => Promise<void> | void;
  userName: string;
  userAvatar?: string;
  userEmail?: string;
  isSubmitting?: boolean;
}

export default function BlockUserModal({
  isOpen,
  onClose,
  onConfirm,
  userName,
  userAvatar,
  userEmail,
  isSubmitting = false,
}: BlockUserModalProps) {
  const { mounted } = useBlockUserModal();

  if (!isOpen || !mounted) return null;

  return createPortal(
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-md p-4 animate-in fade-in duration-200">
      <div 
        className="bg-surface/90 backdrop-blur-2xl rounded-[2rem] w-full max-w-[460px] flex flex-col overflow-hidden shadow-2xl border border-glass-border animate-in zoom-in-95 duration-200"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between p-5 border-b border-glass-border bg-surface-solid/50">
          <div className="flex items-center gap-3">
            <div className="size-10 rounded-xl bg-red-500/10 border border-red-500/20 flex items-center justify-center text-red-500 shrink-0 shadow-sm">
              <Ban className="size-5" />
            </div>
            <div className="flex flex-col">
              <h3 className="font-semibold text-base text-txt-primary leading-tight">
                Chặn người dùng
              </h3>
              <p className="text-xs text-txt-extra mt-0.5">
                Xác nhận hành động chặn
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            disabled={isSubmitting}
            className="p-1.5 hover:bg-hover text-txt-extra hover:text-txt-primary rounded-xl transition-colors cursor-pointer disabled:opacity-50"
          >
            <X size={18} />
          </button>
        </div>

        {/* Content */}
        <div className="p-5 flex flex-col gap-4">
          {/* Target user info */}
          <div className="flex items-center gap-3 p-3 rounded-2xl bg-input-surface border border-glass-border shadow-inner">
            <Image
              src={userAvatar || DEFAULT_AVATAR}
              alt={userName || "avatar"}
              width={44}
              height={44}
              className="rounded-full object-cover size-11 shrink-0 shadow-sm border border-glass-border"
            />
            <div className="flex flex-col min-w-0">
              <span className="text-sm font-semibold text-txt-primary truncate">
                {userName}
              </span>
              {userEmail && (
                <span className="text-xs text-txt-extra truncate">
                  {userEmail}
                </span>
              )}
            </div>
          </div>

          {/* Explanation & consequences */}
          <div className="bg-red-500/5 border border-red-500/15 rounded-2xl p-4 flex flex-col gap-2.5">
            <div className="flex items-center gap-2 text-xs font-semibold text-red-500">
              <ShieldAlert className="size-4 shrink-0" />
              <span>Khi bạn chặn người này:</span>
            </div>
            <ul className="text-xs text-txt-secondary space-y-1.5 list-disc list-inside pl-1">
              <li>Người này sẽ không thể gửi tin nhắn cho bạn.</li>
              <li>Bạn cũng sẽ không thể gửi tin nhắn trong cuộc trò chuyện này.</li>
              <li>Trạng thái hoạt động (online) của cả hai sẽ bị ẩn.</li>
              <li>Bạn có thể mở chặn bất kỳ lúc nào tại mục thông tin cuộc trò chuyện.</li>
            </ul>
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 p-4 border-t border-glass-border bg-surface-solid/50">
          <button
            type="button"
            onClick={onClose}
            disabled={isSubmitting}
            className="px-4 py-2.5 text-sm font-medium text-txt-primary hover:bg-hover rounded-xl transition-colors cursor-pointer disabled:opacity-50"
          >
            Hủy
          </button>
          <button
            type="button"
            onClick={onConfirm}
            disabled={isSubmitting}
            className="px-5 py-2.5 text-sm font-medium text-white bg-gradient-to-r from-red-500 to-red-600 hover:from-red-600 hover:to-red-700 rounded-xl disabled:opacity-50 disabled:shadow-none disabled:cursor-not-allowed transition-all shadow-md hover:shadow-lg hover:-translate-y-0.5 cursor-pointer flex items-center gap-2"
          >
            {isSubmitting ? (
              <>
                <div className="size-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                <span>Đang xử lý...</span>
              </>
            ) : (
              <span>Chặn người dùng</span>
            )}
          </button>
        </div>
      </div>
    </div>,
    document.body
  );
}
