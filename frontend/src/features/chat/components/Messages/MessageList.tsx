"use client";
import { useMessageList } from "@/features/chat/hooks/useMessageList";
import MessageItem from "./MessageItem";
import { Loader2 } from "lucide-react";

const MessageList = () => {
    const {
        messages,
        isLoadingMessages,
        hasMoreMessages,
        ref
    } = useMessageList();
    return (
        <div className="flex-1 overflow-y-auto p-4 flex flex-col-reverse space-y-reverse space-y-4">
            {messages.map((msg) => (
                <MessageItem
                    key={msg._id || (msg as any).id}
                    message={msg}
                />
            ))}

            {hasMoreMessages && messages.length > 0 && (
                <div ref={ref} className="h-4 flex items-center justify-center shrink-0">
                    {isLoadingMessages && <Loader2 className="w-5 h-5 animate-spin text-gray-400" />}
                </div>
            )}
        </div>
    );
};

export default MessageList;
