"use client";
import { useState, useEffect } from "react";
import { X, Search, ShieldCheck, Check } from "lucide-react";
import Image from "next/image";
import { useQueryClient } from "@tanstack/react-query";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { DEFAULT_AVATAR } from "@/utils/constants";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { conversationApi } from "@/features/chat/api/conversation.api";
import { useUserStore } from "@/features/chat/stores/userStore";

interface AssignAdminModalProps {
  isOpen: boolean;
  onClose: () => void;
}

export default function AssignAdminModal({
  isOpen,
  onClose,
}: AssignAdminModalProps) {
  const { activeConversation, setActiveConversation } = useChatStore();
  const { user } = useAuthStore();
  const queryClient = useQueryClient();

  const [selectedAdminIds, setSelectedAdminIds] = useState<string[]>([]);
  const [searchQuery, setSearchQuery] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [errorMsg, setErrorMsg] = useState("");

  useEffect(() => {
    if (isOpen) {
      setSelectedAdminIds([]);
      setSearchQuery("");
      setIsSubmitting(false);
      setErrorMsg("");
    }
  }, [isOpen]);

  if (!isOpen || !activeConversation) return null;

  // Filter members who are NOT already admins
  const currentAdminIds = new Set(
    (activeConversation.admin_ids || [])
  );

  const nonAdminMembers = (activeConversation.member_ids || []).filter((m: string) => {
    const mId = m as string;
    return mId && !currentAdminIds.has(mId);
  });

  const filteredMembers = nonAdminMembers.filter((m) => {
    if (!searchQuery.trim()) return true;
    const q = searchQuery.toLowerCase();
    const { users } = useUserStore.getState(); const user = users[m as string]; return user?.name?.toLowerCase().includes(q) || user?.email?.toLowerCase().includes(q);
  });

  const toggleSelectUser = (userId: string) => {
    setErrorMsg("");
    setSelectedAdminIds((prev) =>
      prev.includes(userId)
        ? prev.filter((id) => id !== userId)
        : [...prev, userId]
    );
  };

  const handleAssignAdmins = async () => {
    if (selectedAdminIds.length === 0 || isSubmitting) return;

    try {
      setIsSubmitting(true);
      setErrorMsg("");

      const convId = activeConversation.id as string || activeConversation.id as string;
      await conversationApi.assignAdmins(convId, selectedAdminIds);

      // Invalidate queries to refresh sidebar and conversation details
      queryClient.invalidateQueries({ queryKey: ["conversations"] });

      // Update activeConversation admins locally
      const newlyPromoted = (activeConversation.member_ids || []).filter((m: string) =>
        selectedAdminIds.includes(m as string)
      );

      setActiveConversation({
        ...activeConversation,
        admin_ids: [...(activeConversation.admin_ids || []), ...newlyPromoted.map((m: string) => m || m)],
      });

      onClose();
    } catch (error: unknown) {
      console.error("Lỗi cấp quyền Admin:", error);
      setErrorMsg(
        (error as Error)?.message || "Đã xảy ra lỗi" || "Không thể cấp quyền Admin"
      );
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-md">
      <div className="bg-surface/90 backdrop-blur-2xl rounded-[2rem] w-[460px] max-h-[85vh] flex flex-col overflow-hidden shadow-2xl border border-glass-border animate-in fade-in zoom-in-95 duration-200">
        {/* Header */}
        <div className="flex items-center justify-between p-5 border-b border-glass-border bg-brand-primary/10">
          <div className="flex items-center gap-3 text-brand-primary font-semibold text-lg">
            <ShieldCheck className="size-5 text-brand-primary" />
            <span>Thăng cấp Quản trị viên (Admin)</span>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 hover:bg-hover text-txt-extra rounded-xl transition-colors"
          >
            <X size={20} />
          </button>
        </div>

        <div className="p-5 flex flex-col gap-4 overflow-y-auto flex-1">
          <p className="text-sm font-medium text-txt-extra">
            Chọn thành viên để cấp quyền Quản trị viên cho nhóm:
          </p>

          {errorMsg && (
            <div className="p-3 bg-red-500/10 border border-red-500/30 rounded-xl text-sm text-red-500 font-medium">
              {errorMsg}
            </div>
          )}

          {/* Search bar */}
          <div className="relative">
            <Search
              size={16}
              className="absolute left-3 top-1/2 -translate-y-1/2 text-ic-primary"
            />
            <input
              type="text"
              placeholder="Tìm kiếm thành viên..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full pl-9 pr-4 py-2.5 text-sm border border-glass-border rounded-xl focus:outline-none focus:ring-2 focus:ring-brand-primary/20 focus:border-brand-primary transition-all bg-input-surface text-txt-primary placeholder:text-txt-extra shadow-inner"
            />
          </div>

          {/* Non-admin Members List */}
          <div className="flex flex-col gap-1 max-h-[240px] overflow-y-auto pr-1">
            {filteredMembers.length === 0 ? (
              <p className="text-center text-xs text-txt-extra py-6">
                Tất cả thành viên đã là Admin hoặc không tìm thấy
              </p>
            ) : (
              filteredMembers.map((mId) => {
                const { users } = useUserStore.getState(); const m = users[mId as string] || { id: mId, name: "Loading...", email: "" };
                
                const memberId = m.id as string;
                const isSelected = selectedAdminIds.includes(memberId);

                return (
                  <div
                    key={memberId}
                    onClick={() => toggleSelectUser(memberId)}
                    className={`flex items-center justify-between p-2.5 rounded-xl cursor-pointer transition-colors border ${
                      isSelected
                        ? "bg-brand-primary/10 border-brand-primary/30"
                        : "hover:bg-hover border-transparent hover:border-glass-border shadow-sm hover:shadow"
                    }`}
                  >
                    <div className="flex items-center gap-3 min-w-0">
                      <Image
                        src={(m as any).avatar_url || DEFAULT_AVATAR}
                        alt="avatar"
                        width={32}
                        height={32}
                        className="rounded-full object-cover size-8 shrink-0 shadow-sm"
                      />
                      <div className="flex flex-col min-w-0">
                        <span className="text-sm font-medium text-txt-primary truncate">
                          {m.name}
                        </span>
                        <span className="text-xs text-txt-extra truncate">
                          {(m as any).email}
                        </span>
                      </div>
                    </div>

                    <div
                      className={`size-5 rounded-md flex items-center justify-center border transition-all ${
                        isSelected
                          ? "bg-brand-primary border-brand-primary text-white"
                          : "border-glass-border bg-surface-solid"
                      }`}
                    >
                      {isSelected && <Check size={13} strokeWidth={3} />}
                    </div>
                  </div>
                );
              })
            )}
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 p-4 border-t border-glass-border bg-surface-solid/50">
          <button
            onClick={onClose}
            className="px-5 py-2.5 text-sm font-medium text-txt-primary hover:bg-hover rounded-xl transition-colors cursor-pointer"
          >
            Hủy
          </button>
          <button
            onClick={handleAssignAdmins}
            disabled={selectedAdminIds.length === 0 || isSubmitting}
            className="px-5 py-2.5 text-sm font-medium text-white bg-gradient-primary hover:shadow-lg rounded-xl disabled:opacity-50 disabled:shadow-none disabled:cursor-not-allowed transition-all shadow-md hover:-translate-y-0.5 cursor-pointer"
          >
            {isSubmitting
              ? "Đang xử lý..."
              : `Cấp quyền Admin (${selectedAdminIds.length})`}
          </button>
        </div>
      </div>
    </div>
  );
}
