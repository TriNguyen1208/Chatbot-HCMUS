"use client";
import {
  Bot,
  Users,
  MessageSquare,
  MessageSquareMore,
  Settings,
  ChevronUp,
  ChevronDown,
} from "lucide-react";
import { TabButton, TabButtonInterface } from "@/components/ui";
import { cn } from "@/utils/cn";
import { useTabButtonList } from "@/features/chat/hooks/useTabButtonList";

const tabButtons: Array<TabButtonInterface> = [
  {
    index: 0,
    icon: Bot,
    label: "HCMUS AI",
    href: "/ai",
  },
  {
    index: 1,
    icon: MessageSquareMore,
    label: "All Chat",
    href: "/chat",
  },
  {
    index: 2,
    icon: Users,
    label: "Groups Chat",
    href: "/group-chat",
  },
  {
    index: 3,
    icon: MessageSquare,
    label: "Direct Chat",
    href: "/direct-chat",
  },
  {
    index: 4,
    icon: Settings,
    label: "Setting",
    href: "/setting",
  },
];

const TabButtonList = () => {
  const { pathname, isCollapsed, setCollapsed } = useTabButtonList();
  return (
    <div
      className={cn(
        "flex flex-col justify-center w-full items-center border-b-[1.35] border-b-border-primary/70",
        isCollapsed && "border-none",
      )}
    >
      <div
        className={cn(
          "grid w-full transition-[grid-template-rows] duration-300 ease-in-out",
          isCollapsed ? "grid-rows-[0fr]" : "grid-rows-[1fr]",
        )}
      >
        <ul
          className={cn(
            "w-full px-3 flex flex-col gap-1 justify-center items-center overflow-hidden min-h-0",
            !isCollapsed && "pt-1 pb-3",
          )}
        >
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
      </div>

      <button
        onClick={() => setCollapsed(!isCollapsed)}
        className={cn(
          "group w-full px-3 py-px flex items-center justify-center text-gray-500 bg-black/6 hover:bg-black/9 hover:text-gray-800 active:bg-black/8 cursor-pointer focus:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-blue-400 transition-[width] duration-400 ease-in-out",
          isCollapsed && "w-[30%] rounded-b-lg py-0",
        )}
        aria-label={isCollapsed ? "Expand menu" : "Collapse menu"}
      >
        {isCollapsed ? (
          <ChevronDown
            size={18}
            className="transition-transform duration-200 group-hover:translate-y-0.5"
          />
        ) : (
          <ChevronUp
            size={18}
            className="transition-transform duration-200 group-hover:-translate-y-0.5"
          />
        )}
      </button>
    </div>
  );
};

export default TabButtonList;
