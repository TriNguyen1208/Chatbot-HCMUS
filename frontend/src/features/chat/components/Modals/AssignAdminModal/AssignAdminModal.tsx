"use client";

import { X, Search, ShieldCheck, Check } from "lucide-react";
import Image from "next/image";
import { DEFAULT_AVATAR } from "@/config/constants";
import { useAssignAdminModal } from "./useAssignAdminModal";

interface AssignAdminModalProps {
  isOpen: boolean;
  onClose: () => void;
}

export default function AssignAdminModal({
  isOpen,
  onClose,
}: AssignAdminModalProps) {
  const {
    activeConversation,
    users,
    selectedAdminIds,
    searchQuery,
    setSearchQuery,
    isSubmitting,
    errorMsg,
    filteredMembers,
    toggleSelectUser,
    handleAssignAdmins,
  } = useAssignAdminModal(isOpen, onClose);

  if (!isOpen || !activeConversation) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-md p-4 animate-in fade-in duration-200">
      <div className="bg-surface/90 backdrop-blur-2xl rounded-[2rem] w-full max-w-[460px] max-h-[85vh] flex flex-col overflow-hidden shadow-2xl border border-glass-border animate-in zoom-in-95 duration-200">
        {/* Header */}
        <div className="flex items-center justify-between p-5 border-b border-glass-border bg-surface-solid/50">
          <div className="flex items-center gap-3">
            <div className="size-10 rounded-xl bg-brand-primary/10 border border-brand-primary/20 flex items-center justify-center text-brand-primary shrink-0 shadow-sm">
              <ShieldCheck className="size-5" />
            </div>
            <div className="flex flex-col">
              <h3 className="font-semibold text-base text-txt-primary leading-tight">
                Thăng cấp Quản trị viên
              </h3>
              <p className="text-xs text-txt-extra mt-0.5">
                Cấp quyền Admin quản lý nhóm cho thành viên
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
              placeholder="Tìm kiếm thành viên..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full pl-10 pr-4 py-2.5 text-sm border border-glass-border rounded-xl focus:outline-none focus:ring-2 focus:ring-brand-primary/20 focus:border-brand-primary transition-all bg-input-surface text-txt-primary placeholder:text-txt-extra shadow-inner"
            />
          </div>

          {/* Non-admin Members List */}
          <div className="flex flex-col gap-1.5 max-h-[240px] overflow-y-auto pr-1">
            {filteredMembers.length === 0 ? (
              <p className="text-center text-xs text-txt-extra py-6">
                Không tìm thấy thành viên phù hợp
              </p>
            ) : (
              filteredMembers.map((mId) => {
                const memberId = mId as string;
                const m = users[memberId] || {
                  id: memberId,
                  name: "Đang tải...",
                  email: "",
                };
                const isSelected = selectedAdminIds.includes(memberId);

                return (
                  <div
                    key={memberId}
                    onClick={() => toggleSelectUser(memberId)}
                    className={`flex items-center justify-between p-2.5 rounded-xl cursor-pointer transition-all border ${
                      isSelected
                        ? "bg-brand-primary/10 border-brand-primary/30 shadow-sm"
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
                          ? "bg-brand-primary border-brand-primary text-white shadow-sm"
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
            {selectedAdminIds.length > 0
              ? `Đã chọn ${selectedAdminIds.length} thành viên`
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
    </div>
  );
}
