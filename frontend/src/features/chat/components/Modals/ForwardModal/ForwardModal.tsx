"use client";

import { X, Search, SendHorizontal, Loader2 } from "lucide-react";
import Image from "next/image";
import { Conversation } from "@/types";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useUserStore } from "@/features/chat/stores/userStore";
import { DEFAULT_AVATAR } from "@/config/constants";
import { useForwardModal } from "./useForwardModal";

const ConversationItem = ({
  conversation,
  isSelected,
  onToggle,
}: {
  conversation: Conversation;
  isSelected: boolean;
  onToggle: () => void;
}) => {
  const { user } = useAuthStore();
  const { users } = useUserStore();

  const otherMemberId = conversation.member_ids?.find((m: string) => m !== user?.id) as string;
  const otherMember = users[otherMemberId];

  const displayName = conversation.name || otherMember?.name || "Người dùng";
  const displayAvatar = conversation.avatar_url || otherMember?.avatar_url || DEFAULT_AVATAR;

  return (
    <div
      className={`flex items-center gap-3 p-3 rounded-xl cursor-pointer transition-colors ${
        isSelected
          ? "bg-brand-primary/10 border-brand-primary/20"
          : "hover:bg-hover border-transparent"
      } border`}
      onClick={onToggle}
    >
      <div className="relative">
        <Image
          src={displayAvatar}
          alt={displayName}
          width={40}
          height={40}
          className="rounded-full object-cover size-10 shadow-sm"
        />
      </div>
      <div className="flex flex-col flex-1 truncate">
        <span className="text-sm font-semibold text-txt-primary truncate">{displayName}</span>
        {conversation.type === "group" && (
          <span className="text-xs text-txt-extra truncate">Nhóm</span>
        )}
      </div>
      <div className="flex items-center justify-center">
        <div
          className={`w-5 h-5 rounded-full border flex items-center justify-center transition-colors ${
            isSelected ? "bg-brand-primary border-brand-primary" : "border-glass-border"
          }`}
        >
          {isSelected && <div className="w-2 h-2 rounded-full bg-white" />}
        </div>
      </div>
    </div>
  );
};

export default function ForwardModal() {
  const {
    isForwardModalOpen,
    closeForwardModal,
    isLoading,
    searchQuery,
    setSearchQuery,
    selectedIds,
    isSending,
    filteredConversations,
    toggleSelect,
    handleSend,
  } = useForwardModal();

  if (!isForwardModalOpen) return null;

  return (
    <div className="fixed inset-0 z-[100] flex items-center justify-center bg-black/40 backdrop-blur-md">
      <div className="bg-surface/90 backdrop-blur-2xl rounded-[2rem] w-[400px] max-h-[85vh] flex flex-col overflow-hidden shadow-2xl border border-glass-border animate-in fade-in zoom-in-95 duration-200">
        {/* Header */}
        <div className="flex items-center justify-between p-5 border-b border-glass-border bg-surface-solid/50">
          <div className="flex items-center gap-3 text-txt-primary font-semibold text-lg">
            <SendHorizontal className="size-5 text-brand-primary" />
            <span>Chuyển tiếp tin nhắn</span>
          </div>
          <button
            onClick={closeForwardModal}
            className="p-1.5 hover:bg-hover text-txt-extra rounded-xl transition-colors cursor-pointer"
          >
            <X size={20} />
          </button>
        </div>

        <div className="p-5 flex flex-col gap-4 overflow-hidden flex-1">
          {/* Search */}
          <div className="relative">
            <Search className="absolute left-3.5 top-1/2 -translate-y-1/2 size-4 text-txt-extra" />
            <input
              type="text"
              placeholder="Tìm kiếm người hoặc nhóm..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full pl-10 pr-4 py-2.5 text-sm border border-glass-border rounded-xl focus:outline-none focus:ring-2 focus:ring-brand-primary/20 focus:border-brand-primary transition-all bg-input-surface text-txt-primary placeholder:text-txt-extra shadow-inner"
            />
          </div>

          <p className="text-xs font-semibold text-txt-extra uppercase tracking-wider">
            Cuộc hội thoại gần đây
          </p>

          {/* List */}
          <div className="flex-1 overflow-y-auto pr-1 space-y-1 -mr-2 scrollbar-thin scrollbar-thumb-glass-border scrollbar-track-transparent">
            {isLoading ? (
              <div className="flex justify-center p-4">
                <Loader2 className="animate-spin text-brand-primary size-6" />
              </div>
            ) : filteredConversations.length === 0 ? (
              <div className="text-center p-4 text-txt-extra text-sm">
                Không tìm thấy hội thoại nào.
              </div>
            ) : (
              filteredConversations.map((conv) => (
                <ConversationItem
                  key={conv.id}
                  conversation={conv}
                  isSelected={selectedIds.includes(conv.id as string)}
                  onToggle={() => toggleSelect(conv.id as string)}
                />
              ))
            )}
          </div>
        </div>

        {/* Footer */}
        <div className="p-5 border-t border-glass-border bg-surface-solid/50 flex justify-between items-center">
          <span className="text-xs text-txt-extra font-medium">
            {selectedIds.length > 0
              ? `Đã chọn ${selectedIds.length} hội thoại`
              : "Chưa chọn hội thoại"}
          </span>
          <button
            onClick={handleSend}
            disabled={selectedIds.length === 0 || isSending}
            className="px-6 py-2 bg-gradient-primary text-white text-sm font-semibold rounded-xl shadow-md hover:shadow-lg disabled:opacity-50 disabled:shadow-none transition-all flex items-center gap-2 cursor-pointer"
          >
            {isSending ? (
              <>
                <Loader2 size={16} className="animate-spin" />
                Đang gửi...
              </>
            ) : (
              "Gửi"
            )}
          </button>
        </div>
      </div>
    </div>
  );
}
