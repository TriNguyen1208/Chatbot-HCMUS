"use client";

import { X, Search, UserMinus, Check, AlertTriangle } from "lucide-react";
import Image from "next/image";
import { DEFAULT_AVATAR } from "@/config/constants";
import { useKickMemberModal } from "./useKickMemberModal";

interface KickMemberModalProps {
  isOpen: boolean;
  onClose: () => void;
}

export default function KickMemberModal({
  isOpen,
  onClose,
}: KickMemberModalProps) {
  const {
    activeConversation,
    users,
    selectedUserIds,
    searchQuery,
    setSearchQuery,
    isSubmitting,
    errorMsg,
    filteredMembers,
    kickableMembers,
    isMinMembersViolation,
    toggleSelectUser,
    handleKickMembers,
  } = useKickMemberModal(isOpen, onClose);

  if (!isOpen || !activeConversation) return null;

  const totalMembers = activeConversation.member_ids?.length || 0;
  const remainingCount = totalMembers - selectedUserIds.length;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-md p-4 animate-in fade-in duration-200">
      <div className="bg-surface/90 backdrop-blur-2xl rounded-[2rem] w-full max-w-[460px] max-h-[85vh] flex flex-col overflow-hidden shadow-2xl border border-glass-border animate-in zoom-in-95 duration-200">
        {/* Header */}
        <div className="flex items-center justify-between p-5 border-b border-glass-border bg-surface-solid/50">
          <div className="flex items-center gap-3">
            <div className="size-10 rounded-xl bg-red-500/10 border border-red-500/20 flex items-center justify-center text-red-500 shrink-0 shadow-sm">
              <UserMinus className="size-5" />
            </div>
            <div className="flex flex-col">
              <h3 className="font-semibold text-base text-txt-primary leading-tight">
                Xóa thành viên khỏi nhóm
              </h3>
              <p className="text-xs text-txt-extra mt-0.5">
                Chọn các thành viên cần xóa
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 hover:bg-hover text-txt-extra hover:text-txt-primary rounded-xl transition-colors cursor-pointer"
          >
            <X size={18} />
          </button>
        </div>

        <div className="p-5 flex flex-col gap-4 overflow-y-auto flex-1">
          {/* Member count info & constraint badge */}
          <div
            className={`p-3.5 rounded-xl border text-xs flex items-center justify-between shadow-inner transition-colors ${
              isMinMembersViolation
                ? "bg-red-500/10 border-red-500/30 text-red-500"
                : "bg-input-surface border-glass-border text-txt-extra"
            }`}
          >
            <div className="flex items-center gap-1.5">
              <span>Tổng số thành viên:</span>
              <strong className="text-txt-primary font-semibold text-sm">
                {totalMembers}
              </strong>
            </div>
            <div className="flex items-center gap-1.5">
              <span>Còn lại sau xóa:</span>
              <strong
                className={`text-sm font-semibold ${
                  isMinMembersViolation
                    ? "text-red-500 font-bold"
                    : "text-txt-primary"
                }`}
              >
                {remainingCount}
              </strong>
            </div>
          </div>

          {isMinMembersViolation && (
            <div className="flex items-center gap-2 p-3 bg-red-500/10 border border-red-500/20 rounded-xl text-xs text-red-500 font-medium animate-in fade-in duration-200">
              <AlertTriangle className="size-4 shrink-0 text-red-500" />
              <span>Nhóm phải duy trì tối thiểu 2 thành viên!</span>
            </div>
          )}

          {errorMsg && (
            <div className="p-3 bg-red-500/10 border border-red-500/20 rounded-xl text-xs text-red-500 font-medium animate-in fade-in duration-200">
              {errorMsg}
            </div>
          )}

          {/* Search bar */}
          <div className="relative">
            <Search
              size={16}
              className="absolute left-3.5 top-1/2 -translate-y-1/2 text-ic-primary"
            />
            <input
              type="text"
              placeholder="Tìm kiếm thành viên cần xóa..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full pl-10 pr-4 py-2.5 text-sm border border-glass-border rounded-xl focus:outline-none focus:ring-2 focus:ring-red-500/20 focus:border-red-500/50 transition-all bg-input-surface text-txt-primary placeholder:text-txt-extra shadow-inner"
            />
          </div>

          {/* Members List */}
          <div className="flex flex-col gap-1.5 max-h-[240px] overflow-y-auto pr-1">
            {filteredMembers.length === 0 ? (
              <p className="text-center text-xs text-txt-extra py-6">
                {kickableMembers.length === 0
                  ? "Không có thành viên nào có thể xóa"
                  : "Không tìm thấy thành viên phù hợp"}
              </p>
            ) : (
              filteredMembers.map((mId) => {
                const memberId = mId as string;
                const m = users[memberId] || {
                  id: memberId,
                  name: "Đang tải...",
                  email: "",
                };
                const isSelected = selectedUserIds.includes(memberId);

                return (
                  <div
                    key={memberId}
                    onClick={() => toggleSelectUser(memberId)}
                    className={`flex items-center justify-between p-2.5 rounded-xl cursor-pointer transition-all border ${
                      isSelected
                        ? "bg-red-500/10 border-red-500/30 shadow-sm"
                        : "hover:bg-hover border-transparent hover:border-glass-border shadow-sm hover:shadow"
                    }`}
                  >
                    <div className="flex items-center gap-3 min-w-0">
                      <Image
                        src={(m as any).avatar_url || DEFAULT_AVATAR}
                        alt={m.name || "avatar"}
                        width={36}
                        height={36}
                        className="rounded-full object-cover size-9 shrink-0 shadow-sm"
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
                          ? "bg-red-500 border-red-500 text-white shadow-sm"
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
        <div className="flex items-center justify-between p-4 border-t border-glass-border bg-surface-solid/50">
          <span className="text-xs text-txt-extra">
            {selectedUserIds.length > 0
              ? `Đã chọn ${selectedUserIds.length} thành viên`
              : "Chưa chọn thành viên"}
          </span>
          <div className="flex items-center gap-2.5">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2.5 text-sm font-medium text-txt-primary hover:bg-hover rounded-xl transition-colors cursor-pointer"
            >
              Hủy
            </button>
            <button
              type="button"
              onClick={handleKickMembers}
              disabled={
                selectedUserIds.length === 0 ||
                isSubmitting ||
                isMinMembersViolation
              }
              className="px-5 py-2.5 text-sm font-medium text-white bg-gradient-to-r from-red-500 to-red-600 hover:from-red-600 hover:to-red-700 rounded-xl disabled:opacity-50 disabled:shadow-none disabled:cursor-not-allowed transition-all shadow-md hover:shadow-lg hover:-translate-y-0.5 cursor-pointer"
            >
              {isSubmitting
                ? "Đang xóa..."
                : `Xóa (${selectedUserIds.length}) thành viên`}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
