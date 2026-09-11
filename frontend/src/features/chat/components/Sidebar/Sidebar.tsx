"use client";
import Header from "./components/Header";
import TabButtonList from "./components/TabButtonList";
import FriendBar from "./components/FriendBar";
import SidebarProfile from "./components/SidebarProfile";
import { useSidebar } from "./useSidebar";

export const Sidebar = () => {
  useSidebar();

  return (
    <aside className="w-full relative flex flex-col h-screen border-r border-r-border-primary bg-glass shadow-sm z-10 transition-colors duration-300 overflow-hidden">
      <Header />
      <TabButtonList />
      <FriendBar />
      <SidebarProfile />
    </aside>
  );
};

export default Sidebar;