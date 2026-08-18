import ChatCard from "@/features/chat/components/ChatCard";
import { useFriendMessageList } from "@/features/chat/hooks/useFriendMessageList";

const FriendMessageList = () => {
  const { 
    conversations, 
    isLoadingConversations,
    activeConversation,
    ref,
    handleConversationClick
  } = useFriendMessageList();

  return (
    <ul className="flex flex-col gap-1 w-full h-full overflow-y-auto overflow-x-hidden scrollbar-thumb-input-surface scrollbar-track-white/40 scroll-smooth scrollbar-thin scrollbar-gutter-stable px-2">
      {conversations.map((conv) => (
        <li key={(conv as any)._id || (conv as any).id} className="w-full">
          <ChatCard 
            conversation={conv} 
            isActive={((activeConversation as any)?._id || (activeConversation as any)?.id) === ((conv as any)._id || (conv as any).id)}
            onClick={() => handleConversationClick(conv)}
          />
        </li>
      ))}
      
      {isLoadingConversations && (
        <li className="w-full flex justify-center py-2">
          <span className="text-xs text-txt-extra">Loading...</span>
        </li>
      )}
      <div ref={ref} className="h-4 w-full" />
    </ul>
  );
}

export default FriendMessageList;