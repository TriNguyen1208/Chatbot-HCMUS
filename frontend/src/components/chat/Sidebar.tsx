import Header from "./Header";
import TabButtonList from "./TabButtonList";

const Sidebar = () => {
  return (
    <aside className="bg-secondary flex flex-col h-screen border-r border-r-border-primary">
      <Header />
      <TabButtonList />
    </aside>
  );
}

export default Sidebar;