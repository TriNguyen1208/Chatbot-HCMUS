"use client";
import { X } from "lucide-react";
import Image from "next/image";
import { useSearchUserModal } from "@/features/chat/hooks/useSearchUserModal";
import { DEFAULT_AVATAR } from "@/utils/constants";

interface Props {
  isOpen: boolean;
  onClose: () => void;
}

export default function SearchUserModal({ isOpen, onClose }: Props) {
  const { users, handleUserClick } = useSearchUserModal(isOpen, onClose);

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-md">
      <div className="bg-surface/90 backdrop-blur-2xl rounded-[2rem] w-[400px] max-h-[80vh] flex flex-col overflow-hidden shadow-2xl border border-glass-border animate-in fade-in zoom-in-95 duration-200">
        <div className="flex items-center justify-between p-5 border-b border-glass-border bg-surface-solid/50">
          <h2 className="font-semibold text-lg text-txt-primary">Tin nhắn mới</h2>
          <button onClick={onClose} className="p-1.5 hover:bg-hover text-txt-extra rounded-xl transition-colors">
            <X size={20} />
          </button>
        </div>
        <div className="p-4 overflow-y-auto flex-1 flex flex-col gap-1">
          {users.length === 0 && <p className="text-center text-sm text-txt-extra my-4">Đang tải...</p>}
          {users.map(u => (
            <div 
              key={u.id} 
              className="flex items-center gap-3 p-3 hover:bg-hover rounded-xl cursor-pointer transition-colors border border-transparent hover:border-glass-border shadow-sm hover:shadow"
              onClick={() => handleUserClick(u.id)}
            >
              <Image 
                src={u.avatar_url || DEFAULT_AVATAR} 
                alt="avatar" 
                width={36} 
                height={36} 
                className="rounded-full object-cover size-9" 
              />
              <div className="flex flex-col">
                <span className="text-sm font-medium text-txt-primary">{u.name}</span>
                <span className="text-xs text-txt-extra">{u.email}</span>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
