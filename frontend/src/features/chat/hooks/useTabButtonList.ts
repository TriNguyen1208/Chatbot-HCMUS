import { useState } from "react";
import { usePathname } from "next/navigation";

export const useTabButtonList = () => {
  const pathname = usePathname();
  
  const [isCollapsed, setCollapsed] = useState(false);

  return {
    pathname,
    isCollapsed,
    setCollapsed
  };
};
