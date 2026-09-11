"use client";
import { useState } from "react";

export const useFriendBar = () => {
    const [isSearchOpen, setIsSearchOpen] = useState(false);

    return {
        isSearchOpen,
        setIsSearchOpen
    };
};
