"use client";
import { Bot, Users, MessageSquareMore, Settings } from "lucide-react";
import { TabButton, TabButtonInterface } from "../ui";
import { usePathname } from "next/navigation";

const tabButtons: Array<TabButtonInterface> = [
  {
    index: 0,
    icon: Bot,
    label: "HCMUS AI",
    href: "/ai",
  },
  {
    index: 1,
    icon: Users,
    label: "Groups Chat",
    href: "/groups-chat",
  },
  {
    index: 2,
    icon: MessageSquareMore,
    label: "Friends Chat",
    href: "/chat",
  },
  {
    index: 3,
    icon: Settings,
    label: "Setting",
    href: "/settings",
  },
];

const TabButtonList = () => {
  const pathname = usePathname();

  return (
    <ul className="w-full px-3 py-1 flex flex-col gap-1 justify-center items-center border-b-[1.35] border-b-border-primary/70">
      {tabButtons.map(({ index, href, icon, label }) => (
        <li key={index} className="w-full">
          <TabButton            
            href={href}
            icon={icon}
            label={label}
            isEntering={pathname === href}
          />
        </li>
      ))}
    </ul>
  );
};

export default TabButtonList;
