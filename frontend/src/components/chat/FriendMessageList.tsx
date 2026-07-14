import ChatCard from "../ui/ChatCard"


const FriendMessageList = () => {
  return (
    <ul className="flex flex-col gap-1 w-full overflow-y-visible overflow-x-hidden scrollbar-thumb-input-surface scrollbar-track-white/40 scroll-smooth scrollbar-thin scrollbar-gutter-stable pl-3 pr-1">
      <li key="1" className="w-full">
        <ChatCard />
      </li>
      <li key="2">
        <ChatCard />
      </li>
      <li key="3">
        <ChatCard />
      </li>
      <li key="4">
        <ChatCard />
      </li>
      <li key="5">
        <ChatCard />
      </li>
      <li key="6">
        <ChatCard />
      </li>
      <li key="7">
        <ChatCard />
      </li>
      <li key="8">
        <ChatCard />
      </li>
    </ul>
  );
}

export default FriendMessageList