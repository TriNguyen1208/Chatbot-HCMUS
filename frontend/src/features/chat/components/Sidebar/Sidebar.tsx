import Header from "./Header";
import TabButtonList from "./TabButtonList";
import FriendBar from "./FriendBar";
import SidebarProfile from "./SidebarProfile";

const Sidebar = () => {
  return (
    <aside className="w-full relative flex flex-col h-screen border-r border-r-border-primary bg-glass shadow-sm z-10 transition-colors duration-300 overflow-hidden">
      <Header />
      <TabButtonList />
      <FriendBar />
      <SidebarProfile />
    </aside>
  );
}

export default Sidebar;