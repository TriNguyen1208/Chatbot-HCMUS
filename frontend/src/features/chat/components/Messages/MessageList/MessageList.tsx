"use client";

import { useMessageList } from "./useMessageList";
import { MessageItem } from "../MessageItem";
import { Loader2 } from "lucide-react";

export const MessageList = () => {
  const {
    messages,
    isLoadingMessages,
    hasMoreMessages,
    ref,
    currentTypingUsers,
    watermarksByMessageId,
    isViewingContext,
    clearContextMessages,
  } = useMessageList();
    return (
        <div className="flex-1 overflow-y-auto p-4 flex flex-col-reverse space-y-reverse space-y-4 relative">
            {isViewingContext && (
                <div className="sticky bottom-2 z-20 mx-auto animate-in fade-in slide-in-from-bottom-2 duration-200">
                    <div className="flex items-center gap-3 px-4 py-2 bg-surface/95 backdrop-blur-xl border border-brand-primary/30 rounded-full shadow-lg text-xs">
                        <span className="text-text-secondary">Đang xem ngữ cảnh tin nhắn cũ</span>
                        <button
                            onClick={clearContextMessages}
                            className="font-medium text-brand-primary hover:underline cursor-pointer"
                        >
                            Quay về tin nhắn mới nhất
                        </button>
                    </div>
                </div>
            )}

            {currentTypingUsers.length > 0 && (
                <div className="flex items-center gap-2 self-start animate-in fade-in zoom-in-95 duration-300">
                    <div className="bg-surface-solid border border-glass-border rounded-full px-4 py-2 flex items-center gap-2 shadow-sm">
                        <span className="text-xs font-medium text-txt-extra">
                            {currentTypingUsers.map(u => u.name).join(', ')} đang gõ
                        </span>
                        <div className="flex gap-1 items-center">
                            <span className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-bounce [animation-delay:-0.3s]"></span>
                            <span className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-bounce [animation-delay:-0.15s]"></span>
                            <span className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-bounce"></span>
                        </div>
                    </div>
                </div>
            )}
            
            {messages.map((msg, index) => {
                const msgId = String(msg.id || (msg as any)._id || '');
                return (
                    <MessageItem
                        key={msgId || index}
                        message={msg}
                        watermarks={watermarksByMessageId[msgId]}
                        isLastMessage={index === 0}
                    />
                );
            })}

            {hasMoreMessages && messages.length > 0 && (
                <div ref={ref} className="h-4 flex items-center justify-center shrink-0">
                    {isLoadingMessages && <Loader2 className="w-5 h-5 animate-spin text-gray-400" />}
                </div>
            )}
        </div>
    );
};

export default MessageList;
