"use client";
import { useChatArea } from "./useChatArea";
import ChatHeader from "./components/ChatHeader";
import MessageList from "../Messages/MessageList";
import ChatInput from "./components/ChatInput";

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
