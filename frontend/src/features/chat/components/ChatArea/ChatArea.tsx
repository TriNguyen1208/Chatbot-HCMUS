"use client";
import { useChatArea } from "@/features/chat/hooks/useChatArea";
import ChatHeader from "./ChatHeader";
import MessageList from "../Messages/MessageList";
import ChatInput from "./ChatInput";

const ChatArea = () => {
  const { activeConversation } = useChatArea();
  if (!activeConversation) return null;

  return (
    <div className="flex flex-col h-full w-full relative">
      <ChatHeader />
      <MessageList />
      <ChatInput />
    </div>
  );
};

export default ChatArea;
