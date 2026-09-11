"use client";
import { useState } from "react";
import { usePathname } from "next/navigation";

export const useTabButtonList = () => {
    const pathname = usePathname();
    const [isCollapsed, setCollapsed] = useState<boolean>(false);

    return {
        pathname,
        isCollapsed,
        setCollapsed
    };
};
