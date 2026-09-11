"use client";
import React from "react";
import { createPortal } from "react-dom";
import { X, Trash2, AlertTriangle, Users } from "lucide-react";
import Image from "next/image";
import { useDisbandGroupModal } from "./useDisbandGroupModal";

export interface DisbandGroupModalProps {
  isOpen: boolean;
  onClose: () => void;
  onConfirm: () => Promise<void> | void;
  groupName: string;
  groupAvatar?: string;
  memberCount?: number;
  isSubmitting?: boolean;
}

export default function DisbandGroupModal({
  isOpen,
  onClose,
  onConfirm,
  groupName,
  groupAvatar,
  memberCount = 0,
  isSubmitting = false,
}: DisbandGroupModalProps) {
  const { mounted } = useDisbandGroupModal();

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
              <Trash2 className="size-5" />
            </div>
            <div className="flex flex-col">
              <h3 className="font-semibold text-base text-txt-primary leading-tight">
                Giải tán nhóm
              </h3>
              <p className="text-xs text-txt-extra mt-0.5">
                Xác nhận giải tán cuộc trò chuyện nhóm
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
          {/* Target Group Info */}
          <div className="flex items-center gap-3.5 p-3 rounded-2xl bg-input-surface border border-glass-border shadow-inner">
            <div className="relative size-12 shrink-0 rounded-full overflow-hidden border border-glass-border shadow-sm">
              {groupAvatar ? (
                <Image
                  src={groupAvatar}
                  alt={groupName || "Group Avatar"}
                  fill
                  className="object-cover"
                />
              ) : (
                <div className="w-full h-full bg-brand-primary/10 flex items-center justify-center text-brand-primary">
                  <Users size={22} />
                </div>
              )}
            </div>
            <div className="flex flex-col min-w-0">
              <span className="text-sm font-semibold text-txt-primary truncate">
                {groupName}
              </span>
              <span className="text-xs text-txt-extra truncate">
                {memberCount > 0 ? `${memberCount} thành viên` : "Nhóm trò chuyện"}
              </span>
            </div>
          </div>

          {/* Warning notice */}
          <div className="bg-red-500/5 border border-red-500/20 rounded-2xl p-4 flex flex-col gap-2.5">
            <div className="flex items-center gap-2 text-xs font-semibold text-red-500">
              <AlertTriangle className="size-4 shrink-0" />
              <span>Cảnh báo quan trọng:</span>
            </div>
            <ul className="text-xs text-txt-secondary space-y-1.5 list-disc list-inside pl-1">
              <li>Hành động này <strong className="text-red-500">không thể hoàn tác</strong>.</li>
              <li>Toàn bộ thành viên sẽ <strong className="text-txt-primary">rời khỏi nhóm</strong> ngay lập tức.</li>
              <li>Cuộc trò chuyện sẽ biến mất khỏi danh sách của tất cả mọi người.</li>
              <li>Không ai có thể gửi thêm tin nhắn vào nhóm này nữa.</li>
            </ul>
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-2.5 p-4 border-t border-glass-border bg-surface-solid/50">
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
                <span>Đang giải tán...</span>
              </>
            ) : (
              <span>Giải tán nhóm</span>
            )}
          </button>
        </div>
      </div>
    </div>,
    document.body
  );
}
