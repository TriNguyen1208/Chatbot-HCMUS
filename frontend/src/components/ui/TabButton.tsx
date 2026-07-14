"use client";
import { TabButtonComponent } from "./types";
import { useState } from "react";
import Link from "next/link";
import { cn } from "@/utils/cn";

const TabButton = ({
  href,
  icon: Icon,
  label,
  isEntering,
}: TabButtonComponent) => {
  const [isHovered, setHover] = useState(false);
  const content = (
    <>
      <Icon
        size={22}
        color={isHovered || isEntering ? "#003D9B" : "#434654"}
        strokeWidth={2.75}
      />
      <p
        className={cn(
          "text-xs text-txt-extra font-semibold",
          isEntering && "text-brand-primary text-sm",
        )}
      >
        {label}
      </p>
    </>
  );

  const sharedClassName = 
  "w-full flex flex-row gap-3 pl-3 py-2 justify-start items-center hover:cursor-pointer hover:bg-hover rounded-lg transition-transform duration-200";

  if (isEntering) {
    return (
      <div
        className={cn(
          sharedClassName,
          "bg-hover border-l-5 border-l-brand-primary rounded-l-none scale-104",
        )}
        aria-current="page"
      >
        {content}
      </div>
    );
  }

  return (
    <Link
      href={href}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      className={cn(sharedClassName, "hover:scale-102 active:scale-98")}
    >
      {content}
    </Link>
  );
};

export default TabButton;
