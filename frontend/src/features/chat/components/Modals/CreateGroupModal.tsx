"use client";
import { X, Search, Users, Check } from "lucide-react";
import Image from "next/image";
import { useCreateGroupModal } from "@/features/chat/hooks/useCreateGroupModal";

interface CreateGroupModalProps {
  isOpen: boolean;
  onClose: () => void;
}

export default function CreateGroupModal({
  isOpen,
  onClose,
}: CreateGroupModalProps) {
  const {
    groupName,
    setGroupName,
    searchQuery,
    setSearchQuery,
    existingMembers,
    filteredUsers,
    selectedUserIds,
    toggleSelectUser,
    handleCreateGroup,
    isSubmitting,
  } = useCreateGroupModal(isOpen, onClose);

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-md">
      <div className="bg-surface/90 backdrop-blur-2xl rounded-[2rem] w-[460px] max-h-[85vh] flex flex-col overflow-hidden shadow-2xl border border-glass-border animate-in fade-in zoom-in-95 duration-200">
        {/* Header */}
        <div className="flex items-center justify-between p-5 border-b border-glass-border bg-surface-solid/50">
          <div className="flex items-center gap-3 text-txt-primary font-semibold text-lg">
            <Users className="size-5 text-brand-primary" />
            <span>Tạo nhóm trò chuyện</span>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 hover:bg-hover text-txt-extra rounded-xl transition-colors"
          >
            <X size={20} />
          </button>
        </div>

        <div className="p-5 flex flex-col gap-4 overflow-y-auto flex-1">
          {/* Tên nhóm */}
          <div>
            <label className="block text-xs font-medium text-txt-extra mb-1.5">
              Tên nhóm
            </label>
            <input
              type="text"
              placeholder="Nhập tên nhóm..."
              value={groupName}
              onChange={(e) => setGroupName(e.target.value)}
              className="w-full px-4 py-2.5 text-sm border border-glass-border rounded-xl focus:outline-none focus:ring-2 focus:ring-brand-primary/20 focus:border-brand-primary transition-all bg-input-surface text-txt-primary placeholder:text-txt-extra shadow-inner"
            />
          </div>

          {/* Thành viên có sẵn */}
          <div>
            <label className="block text-xs font-medium text-txt-extra mb-1.5">
              Thành viên hiện tại ({existingMembers.length})
            </label>
            <div className="flex flex-wrap gap-2 p-2.5 bg-input-surface rounded-xl border border-glass-border shadow-inner">
              {existingMembers.map((m) => (
                  <div
                  key={m.id || (m as any)._id}
                    className="flex items-center gap-1.5 bg-surface px-2.5 py-1.5 rounded-full text-xs border border-glass-border shadow-sm text-txt-primary font-medium"
                  >
                    <Image
                      src={m.avatar_url || "/CR_LM_Chess.jpg"}
                      alt={m.name || "User"}
                      width={18}
                      height={18}
                      className="rounded-full size-4 object-cover"
                    />
                    <span className="max-w-[100px] truncate">{m.name}</span>
                  </div>
              ))}
            </div>
          </div>

          {/* Search bar */}
          <div>
            <label className="block text-xs font-medium text-txt-extra mb-1.5">
              Thêm thành viên khác
            </label>
            <div className="relative">
              <Search
                size={16}
                className="absolute left-3 top-1/2 -translate-y-1/2 text-ic-primary"
              />
              <input
                type="text"
                placeholder="Tìm kiếm người dùng..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="w-full pl-9 pr-4 py-2.5 text-sm border border-glass-border rounded-xl focus:outline-none focus:ring-2 focus:ring-brand-primary/20 focus:border-brand-primary transition-all bg-input-surface text-txt-primary placeholder:text-txt-extra shadow-inner"
              />
            </div>
          </div>

          {/* User List */}
          <div className="flex flex-col gap-1 max-h-[220px] overflow-y-auto pr-1">
            {filteredUsers.length === 0 ? (
              <p className="text-center text-xs text-txt-extra py-6">
                Không tìm thấy người dùng phù hợp
              </p>
            ) : (
              filteredUsers.map((u) => {
                const uId = (u.id || (u as any)._id) as string;
                const isSelected = selectedUserIds.includes(uId);

                return (
                  <div
                    key={uId}
                    onClick={() => toggleSelectUser(uId)}
                    className={`flex items-center justify-between p-2.5 rounded-xl cursor-pointer transition-colors border ${
                      isSelected
                        ? "bg-brand-primary/10 border-brand-primary/30"
                        : "hover:bg-hover border-transparent hover:border-glass-border shadow-sm hover:shadow"
                    }`}
                  >
                    <div className="flex items-center gap-3 min-w-0">
                      <Image
                        src={u.avatar_url || "/CR_LM_Chess.jpg"}
                        alt="avatar"
                        width={32}
                        height={32}
                        className="rounded-full object-cover size-8 shrink-0 shadow-sm"
                      />
                      <div className="flex flex-col min-w-0">
                        <span className="text-sm font-medium text-txt-primary truncate">
                          {u.name}
                        </span>
                        <span className="text-xs text-txt-extra truncate">
                          {u.email}
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
            Huỷ
          </button>
          <button
            onClick={handleCreateGroup}
            disabled={selectedUserIds.length === 0 || isSubmitting}
            className="px-5 py-2.5 text-sm font-medium text-white bg-gradient-primary rounded-xl disabled:opacity-50 disabled:cursor-not-allowed transition-all shadow-md hover:shadow-lg hover:-translate-y-0.5 cursor-pointer"
          >
            {isSubmitting
              ? "Đang tạo..."
              : `Tạo nhóm (${selectedUserIds.length})`}
          </button>
        </div>
      </div>
    </div>
  );
}
