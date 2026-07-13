'use client';
import { TabButtonInterface } from "./types";
import { useState } from "react";

const TabButton = ({ index, icon: Icon, label }: TabButtonInterface) => {
  const [isHovered, setHover] = useState(false);
  return (
    <div
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      className="w-full flex flex-row gap-3 pl-3 py-2 justify-start items-center hover:cursor-pointer hover:bg-blue-300 rounded-lg"
    >
      <Icon size={24} color={isHovered ? "#0040A2" : "#D9D4D4"} />
      <p className="text-xs text-black/50 font-normal">{label}</p>
    </div>
  );
}

export default TabButton;