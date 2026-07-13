import Header from "./Header";
import TabButtonList from "./TabButtonList";

const Sidebar = () => {
  return (
    <aside>
      <Header />
      <div className="h-0.5 w-full bg-black/20"></div>
      <TabButtonList />
    </aside>
  )
}

export default Sidebar;