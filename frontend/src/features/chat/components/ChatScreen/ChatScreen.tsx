"use client";
import { Suspense } from "react";
import { EmptyChatScreen } from "@/components/ui";
import { ChatArea } from "@/features/chat/components";
import { useChatScreen } from "@/features/chat/hooks/useChatScreen";

export interface ChatScreenProps {
  type?: "utu" | "group" | "all";
}

const ChatPageContent = ({ type }: ChatScreenProps) => {
  const { activeConversation } = useChatScreen(type);
  return activeConversation ? <ChatArea /> : <EmptyChatScreen />;
}

const ChatScreen = ({ type }: ChatScreenProps) => {
  return (
    <Suspense fallback={<div className="w-full h-full flex items-center justify-center">Loading...</div>}>
      <ChatPageContent type={type} />
    </Suspense>
  )
}

export default ChatScreen;
