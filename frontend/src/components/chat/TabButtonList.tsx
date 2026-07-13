'use client';
import { Bot, Users, MessageSquareMore, Settings } from "lucide-react";
import {TabButton, TabButtonInterface } from "../ui";

const tabButtons: Array<TabButtonInterface> = [
  {
    index: 0,
    icon: Bot,
    label: "HCMUS AI"
  },
  {
    index: 1,
    icon: Users,
    label: "Groups Chat",
  },
  {
    index: 2,
    icon: MessageSquareMore,
    label: "Friends Chat",
  },
  {
    index: 3,
    icon: Settings,
    label: "Setting"
  }
];

const TabButtonList = () => {
  return <ul className="w-full px-5 py-2 flex flex-col gap-2 justify-center items-center">
    {tabButtons.map(({index, icon, label}) => (<TabButton key={index} index={index} icon={icon} label={label} />))}
  </ul>;
};

export default TabButtonList;
