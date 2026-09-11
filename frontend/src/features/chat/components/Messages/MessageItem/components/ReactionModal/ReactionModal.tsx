import Image from "next/image";
import { X } from "lucide-react";
import { DEFAULT_AVATAR } from "@/config/constants";
import type { Message } from "@/types";
import { useReactionModal } from "./useReactionModal";

export interface ReactionModalProps {
  message: Message;
  onClose: () => void;
}

export const ReactionModal = ({ message, onClose }: ReactionModalProps) => {
  const { users } = useReactionModal(message);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm"
      onClick={onClose}
    >
      <div
        className="bg-surface-solid w-full max-w-sm rounded-2xl shadow-xl overflow-hidden"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between p-4 border-b border-glass-border">
          <h3 className="font-semibold text-txt-primary">Reactions</h3>
          <button
            onClick={onClose}
            className="p-1 hover:bg-hover rounded-full text-txt-extra transition-colors cursor-pointer"
          >
            <X size={20} />
          </button>
        </div>
        <div className="p-2 max-h-64 overflow-y-auto">
          {message.reactions?.map((reaction, idx) => {
            const user = users[reaction.user_id || ""];
            return (
              <div
                key={idx}
                className="flex items-center justify-between p-2 hover:bg-hover rounded-xl transition-colors"
              >
                <div className="flex items-center gap-3">
                  <Image
                    src={user?.avatar_url || DEFAULT_AVATAR}
                    alt="avatar"
                    width={36}
                    height={36}
                    className="rounded-full object-cover size-9"
                  />
                  <span className="font-medium text-txt-primary text-sm">
                    {user?.name || "Người dùng"}
                  </span>
                </div>
                <span className="text-2xl">{reaction.emoji}</span>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
};

export default ReactionModal;
