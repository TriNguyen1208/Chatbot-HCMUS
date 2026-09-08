"use client";
import { Suspense } from "react";
import { EmptyChatScreen } from "@/components/ui";
import { ChatArea } from "@/features/chat/components";
import { useChatScreen } from "@/features/chat/hooks/useChatScreen";

export interface ChatScreenProps {
  type?: "utu" | "group" | "all";
}

import ConversationInfo from "../ChatArea/ConversationInfo";
import { useModalStore } from "@/features/chat/stores/modalStore";
import CreateGroupModal from "../Modals/CreateGroupModal";
import UserProfileModal from "../Modals/UserProfileModal";
import KickMemberModal from "../Modals/KickMemberModal";
import AssignAdminModal from "../Modals/AssignAdminModal";
import ForwardModal from "../Modals/ForwardModal";

const ChatPageContent = ({ type }: ChatScreenProps) => {
  const { activeConversation } = useChatScreen(type);
  const {
    isCreateGroupOpen, setCreateGroupOpen,
    isKickModalOpen, setKickModalOpen,
    isAssignAdminModalOpen, setAssignAdminModalOpen
  } = useModalStore();

  if (!activeConversation) return <EmptyChatScreen />;

  return (
    <>
      <div className="flex flex-row w-full h-full overflow-hidden relative">
        <div className="flex-1 min-w-0">
          <ChatArea />
        </div>
        <ConversationInfo />
      </div>

      <CreateGroupModal
        isOpen={isCreateGroupOpen}
        onClose={() => setCreateGroupOpen(false)}
      />

      <KickMemberModal
        isOpen={isKickModalOpen}
        onClose={() => setKickModalOpen(false)}
      />

      <AssignAdminModal
        isOpen={isAssignAdminModalOpen}
        onClose={() => setAssignAdminModalOpen(false)}
      />

      <UserProfileModal />
      <ForwardModal />
    </>
  );
}

const ChatScreen = ({ type }: ChatScreenProps) => {
  return (
    <Suspense fallback={<div className="w-full h-full flex items-center justify-center">Loading...</div>}>
      <ChatPageContent type={type} />
    </Suspense>
  )
}

export default ChatScreen;
