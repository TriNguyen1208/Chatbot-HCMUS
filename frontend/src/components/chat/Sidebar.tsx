import Header from "./Header";
import TabButtonList from "./TabButtonList";
import FriendBar from "./FriendBar";
const Sidebar = () => {
  return (
    <aside className="bg-secondary w-full flex flex-col h-screen border-r border-r-border-primary">
      <Header />
      <TabButtonList />
      <FriendBar />
    </aside>
  );
}

export default Sidebar;